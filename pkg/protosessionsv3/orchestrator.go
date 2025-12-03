package protosessionsv3

import (
	"context"
	"sort"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/monitoring"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type Orchestrator struct {
	ctx              context.Context
	backend          BatchedIOBackend
	closer           Closer
	requeuer         receiver.Storage
	settingsRegistry properties.SettingsRegistry
	timingWheel      *TimingWheel
	evictionStrategy EvictionStrategy

	nextBucketRequests  chan []*GetAllProtosessionsForBucketRequest
	nextBucketResponses chan []*GetAllProtosessionsForBucketResponse

	processBucketAlreadyRan bool

	processBatchHist     metric.Float64Histogram
	conflictCheckHist    metric.Float64Histogram
	handleBatchHist      metric.Float64Histogram
	cleanupHist          metric.Float64Histogram
	processBucketHist    metric.Float64Histogram
	getProtosessionsHist metric.Float64Histogram
	closeHist            metric.Float64Histogram
	requeueHist          metric.Float64Histogram
	hitsReceivedCounter  metric.Int64Counter
	hitsClosedCounter    metric.Int64Counter
	evictionsCounter     metric.Int64Counter
	processingLagGauge   metric.Float64Gauge

	lastLagWarn time.Time
}

var (
	processBatchHist     metric.Float64Histogram
	conflictCheckHist    metric.Float64Histogram
	handleBatchHist      metric.Float64Histogram
	cleanupHist          metric.Float64Histogram
	processBucketHist    metric.Float64Histogram
	getProtosessionsHist metric.Float64Histogram
	closeHist            metric.Float64Histogram
	requeueHist          metric.Float64Histogram
	hitsReceivedCounter  metric.Int64Counter
	hitsClosedCounter    metric.Int64Counter
	evictionsCounter     metric.Int64Counter
	processingLagGauge   metric.Float64Gauge
)

func init() { //nolint:funlen // metrics initialization
	meter := otel.GetMeterProvider().Meter("protosessions")

	processBatchHist, _ = meter.Float64Histogram(
		"protosessions.process_batch.duration",
		metric.WithDescription("Batch processing duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	conflictCheckHist, _ = meter.Float64Histogram(
		"protosessions.conflict_check.duration",
		metric.WithDescription("Conflict detection duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	handleBatchHist, _ = meter.Float64Histogram(
		"protosessions.handle_batch.duration",
		metric.WithDescription("Batch handling duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	cleanupHist, _ = meter.Float64Histogram(
		"protosessions.cleanup.duration",
		metric.WithDescription("Cleanup operations duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	processBucketHist, _ = meter.Float64Histogram(
		"protosessions.process_bucket.duration",
		metric.WithDescription("Bucket processing duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	getProtosessionsHist, _ = meter.Float64Histogram(
		"protosessions.get_protosessions.duration",
		metric.WithDescription("Fetching protosessions for bucket"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	closeHist, _ = meter.Float64Histogram(
		"protosessions.close.duration",
		metric.WithDescription("Closing proto-session duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	requeueHist, _ = meter.Float64Histogram(
		"protosessions.requeue.duration",
		metric.WithDescription("Requeuing hits duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	hitsReceivedCounter, _ = meter.Int64Counter(
		"protosessions.hits.received",
		metric.WithDescription("Hits received for live processing"),
	)
	hitsClosedCounter, _ = meter.Int64Counter(
		"protosessions.hits.closed",
		metric.WithDescription("Hits closed when bucket expires"),
	)
	evictionsCounter, _ = meter.Int64Counter(
		"protosessions.evictions",
		metric.WithDescription("Evictions triggered"),
	)
	processingLagGauge, _ = meter.Float64Gauge(
		"protosessions.processing.lag",
		metric.WithDescription("Processing lag in seconds"),
		metric.WithUnit("s"),
	)
}

func NewOrchestrator(
	ctx context.Context,
	backend BatchedIOBackend,
	tickerStateBackend TimingWheelStateBackend,
	closer Closer,
	requeuer receiver.Storage,
	settingsRegistry properties.SettingsRegistry,
	evictionStrategy EvictionStrategy,
) *Orchestrator {
	o := &Orchestrator{
		ctx:                     ctx,
		backend:                 backend,
		closer:                  closer,
		requeuer:                requeuer,
		settingsRegistry:        settingsRegistry,
		evictionStrategy:        evictionStrategy,
		processBucketAlreadyRan: false,
		nextBucketRequests:      make(chan []*GetAllProtosessionsForBucketRequest),
		nextBucketResponses:     make(chan []*GetAllProtosessionsForBucketResponse),

		// metrics
		processBatchHist:     processBatchHist,
		conflictCheckHist:    conflictCheckHist,
		handleBatchHist:      handleBatchHist,
		cleanupHist:          cleanupHist,
		processBucketHist:    processBucketHist,
		getProtosessionsHist: getProtosessionsHist,
		closeHist:            closeHist,
		requeueHist:          requeueHist,
		hitsReceivedCounter:  hitsReceivedCounter,
		hitsClosedCounter:    hitsClosedCounter,
		evictionsCounter:     evictionsCounter,
		processingLagGauge:   processingLagGauge,
	}
	o.timingWheel = NewTimingWheel(
		tickerStateBackend,
		1*time.Second,
		o.processBucket,
	)
	go o.timingWheel.Start(ctx)
	go o.prefillNextBuckets()
	return o
}

func (o *Orchestrator) processBatch(
	ctx context.Context,
	hitsBatch []*hits.Hit,
) *ProtosessionError {
	// Dear reader, welcome to the heart of this project.
	// Here we receive a batch of hits and need to glue each of them to its session. From bird's eye view:
	// 1. We check if hits have identifier conflicts with other sessions.
	// 2. We append hits to proto-sessions (loosely connected collection of hits,
	// which may or may not form a session in the future).
	// 3. If we chose to evict conflicting hits, we make sure to clean them up.
	// All of the above will be described below, let's go!
	logrus.Infof("Appending to proto-sessions hits batch of size `%d`", len(hitsBatch))
	processBatchStart := time.Now()
	defer func() {
		o.processBatchHist.Record(ctx, time.Since(processBatchStart).Seconds())
	}()

	if len(hitsBatch) == 0 {
		return nil
	}
	// At this point, batch can contain hits from different properties, so we prepare a
	// settings registry, that will help us determine some metadata about the hits, for example
	// how long does the session for this hit should last.
	batchSettingsRegistry := o.settingsRegistry

	// A preparation for eviction logic starts here. First we need to define what AuthoritativeClientID is.
	// It's the ClientID the session will be attributed to. More about different identifiers can be
	// found in our tech docs. Eviction in TLDR is a process of changing the
	// AuthoritativeClientID of a hit to the ID of the session that it conflicts with, effectively
	// changing the ownership of the hit.
	//
	// It may happen, that a hit evicted in the past from another partition comes
	// here and the session it should be attributed to is already closed.
	// This logic is not relevant in OSS, but it's important for Cloud, which supports partitioning.
	newBatch, hitsToDrop, err := o.seedOutdatedHits(hitsBatch, batchSettingsRegistry)
	if err != nil {
		return NewProtosessionError(err, true)
	}

	// Now we have a list of hits, that are not being processed by the timing wheel yet.
	// We need to check if they have identifier conflicts with other sessions.
	// The BatchingIOBackend interface allows efficient batching of I/O operations for this orchestrator.
	conflicts, err := o.checkIdentifierConflicts(ctx, newBatch, batchSettingsRegistry)
	if err != nil {
		return NewProtosessionError(err, true)
	}

	// Apply eviction strategy to conflicting hits. This orchestrator uses strategy pattern to allow for different
	// eviction strategies. The `RewriteIDAndUpdateInPlaceStrategy` just updates the AuthoritativeClientID
	// and continues normal processing. The `EvictWholeProtosessionStrategy` evicts the entire proto-session
	// and re-queues it (it's usable when we have for example partitioning and are not sure if the conflicting
	// session is on the same partition or not). In OSS, we only use `RewriteIDAndUpdateInPlaceStrategy`.
	hitsToBeSaved, protosessionsForEviction, evictionCount := o.applyEvictionStrategy(newBatch, conflicts)
	if evictionCount > 0 {
		o.evictionsCounter.Add(ctx, evictionCount)
	}

	// Here we're planning to persist all the hits in the batch to their proto-sessions. There are three operations
	// involved:
	// * Appending hits to proto-sessions - this is quite straightforward, we take the AuthoritativeClientID and
	// the hit and append it to the proto-session.
	// * Marking proto-sessions for closing - we mark the timing wheel bucket (the future time) when we (currently)
	// expect the session to be closed. Consecutive hits with the same AuthoritativeClientID can invalidate the current
	// bucket, the logic that handles this is in the Backend (for example BoltDB backend keeps a map in memory with last
	// bucket number for each AuthoritativeClientID, another approach would be to update the value in the database).
	// * Fetching evicted proto-session hits - we fetch the hits from the proto-session that are evicted, the requests
	// are generated by the eviction strategy - for most cases here will be empty.
	appendReqs, markReqs, getEvictedReqs, err := o.buildSaveRequests(
		hitsToBeSaved, protosessionsForEviction, batchSettingsRegistry,
	)
	if err != nil {
		return NewProtosessionError(err, true)
	}

	// Execute the batch: append hits, fetch evicted proto-session hits, mark buckets.
	handleBatchStart := time.Now()
	appendResps, getEvictedResps, markResps := o.backend.HandleBatch(ctx, appendReqs, getEvictedReqs, markReqs)
	o.handleBatchHist.Record(ctx, time.Since(handleBatchStart).Seconds())

	if err := o.checkHandleBatchResponses(appendResps, markResps); err != nil {
		return err
	}

	// Collect all hits from evicted proto-sessions (both new hits and existing ones fetched from storage)
	// and re-queue them so they can be processed again with correct AuthoritativeClientID.
	if err := o.processEvictedProtosessions(ctx, getEvictedResps, protosessionsForEviction); err != nil {
		return err
	}

	// Cleanup phase: remove evicted proto-session data and metadata for outdated, dropped hits.
	if err := o.cleanupDroppedAndEvicted(ctx, hitsToDrop, protosessionsForEviction, batchSettingsRegistry); err != nil {
		return err
	}

	// Update the timing wheel time to the last hit in the batch. The timing wheel does not use absolute time,
	// instead it tracks the processing progress in buckets. This updates the timing wheel's current time,
	// which determines which buckets are safe to process (buckets before the current bucket). The timing wheel
	// advances independently via its tick loop, not immediately upon this update. If multiple batches contain
	// hits from the same session, each batch updates the timing wheel to its latest hit time, ensuring the
	// timing wheel's current bucket reflects the most recent processing progress.
	if len(newBatch) > 0 {
		o.timingWheel.UpdateTime(newBatch[len(newBatch)-1].ServerReceivedTime)
		o.hitsReceivedCounter.Add(ctx, int64(len(newBatch)))
	}
	return nil
}

// seedOutdatedHits partitions hits into timing wheel buckets, locks each bucket, and filters out
// hits destined for buckets that are currently being processed or already expired.
func (o *Orchestrator) seedOutdatedHits(
	hitsBatch []*hits.Hit,
	registry properties.SettingsRegistry,
) ([]*hits.Hit, map[hits.ClientID]*hits.Hit, error) {
	uniqueBuckets := make(map[int64][]*hits.Hit)
	for _, hit := range hitsBatch {
		settings, err := registry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return nil, nil, err
		}
		bucketNumber := o.timingWheel.BucketNumber(hit.ServerReceivedTime.Add(settings.SessionDuration))
		uniqueBuckets[bucketNumber] = append(uniqueBuckets[bucketNumber], hit)
	}

	newBatch := []*hits.Hit{}
	hitsToDrop := map[hits.ClientID]*hits.Hit{}

	for bucketNumber, bucketHits := range uniqueBuckets {
		// Check if the session is currently being closed - like NOW. If so, we drop the hits,
		// because we have nothing else to do with them - it's data loss.
		ok := o.timingWheel.lock.TryLock(bucketNumber)
		if !ok {
			logrus.Warnf("Dropping %d hits for bucket %d because it is being processed", len(bucketHits), bucketNumber)
			for _, hit := range bucketHits {
				hitsToDrop[hit.AuthoritativeClientID] = hit
			}
			continue
		}
		defer o.timingWheel.lock.Drop(bucketNumber) //nolint:gocritic // it's ok, no resource leaks

		// Grabbed the lock, now check if the bucket is already expired.
		if bucketNumber <= o.timingWheel.BucketNumber(o.timingWheel.CurrentTime()) {
			currentBucket := o.timingWheel.BucketNumber(o.timingWheel.CurrentTime())
			logrus.Warnf(
				"Dropping %d hits for bucket %d because it is already expired (current bucket: %d)",
				len(bucketHits), bucketNumber, currentBucket,
			)
			for _, hit := range bucketHits {
				hitsToDrop[hit.AuthoritativeClientID] = hit
			}
			continue
		}
		newBatch = append(newBatch, bucketHits...)
	}

	return newBatch, hitsToDrop, nil
}

// checkIdentifierConflicts queries the backend for identifier conflicts across all hits in the batch.
func (o *Orchestrator) checkIdentifierConflicts(
	ctx context.Context,
	newBatch []*hits.Hit,
	registry properties.SettingsRegistry,
) (map[hits.ClientID]*IdentifierConflictResponse, error) {
	var requests []*IdentifierConflictRequest
	for _, hit := range newBatch {
		settings, err := registry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return nil, err
		}
		requests = append(requests, GetConflictCheckRequests(hit, settings)...)
	}

	conflictCheckStart := time.Now()
	results := o.backend.GetIdentifierConflicts(ctx, requests)
	o.conflictCheckHist.Record(ctx, time.Since(conflictCheckStart).Seconds())

	conflicts := make(map[hits.ClientID]*IdentifierConflictResponse)
	for _, result := range results {
		if result.HasConflict {
			conflicts[result.Request.Hit.AuthoritativeClientID] = result
		}
	}
	return conflicts, nil
}

// applyEvictionStrategy separates hits into those to save and those requiring eviction.
func (o *Orchestrator) applyEvictionStrategy(
	newBatch []*hits.Hit,
	conflicts map[hits.ClientID]*IdentifierConflictResponse,
) (hitsToBeSaved []*hits.Hit, protosessionsForEviction map[hits.ClientID][]*hits.Hit, evictionCount int64) {
	protosessionsForEviction = make(map[hits.ClientID][]*hits.Hit)
	hitsToBeSaved = make([]*hits.Hit, 0)
	evictionCount = 0

	for _, hit := range newBatch {
		if conflict, ok := conflicts[hit.AuthoritativeClientID]; ok {
			o.evictionStrategy(hit, conflict, &hitsToBeSaved, protosessionsForEviction)
			evictionCount++
		} else {
			hitsToBeSaved = append(hitsToBeSaved, hit)
		}
	}
	return hitsToBeSaved, protosessionsForEviction, evictionCount
}

// buildSaveRequests creates backend requests for appending hits and marking bucket closings.
func (o *Orchestrator) buildSaveRequests(
	hitsToBeSaved []*hits.Hit,
	protosessionsForEviction map[hits.ClientID][]*hits.Hit,
	registry properties.SettingsRegistry,
) (
	[]*AppendHitsToProtoSessionRequest,
	[]*MarkProtoSessionClosingForGivenBucketRequest,
	[]*GetProtoSessionHitsRequest,
	error,
) {
	appendReqs := make([]*AppendHitsToProtoSessionRequest, 0, len(hitsToBeSaved))
	markReqs := make([]*MarkProtoSessionClosingForGivenBucketRequest, 0, len(hitsToBeSaved))

	for _, hit := range hitsToBeSaved {
		settings, err := registry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return nil, nil, nil, err
		}
		markReqs = append(markReqs, NewMarkProtoSessionClosingForGivenBucketRequest(
			hit.AuthoritativeClientID,
			o.timingWheel.BucketNumber(hit.ServerReceivedTime.Add(settings.SessionDuration)),
		))
		appendReqs = append(appendReqs, NewAppendHitsToProtoSessionRequest(
			hit.AuthoritativeClientID,
			[]*hits.Hit{hit},
		))
	}

	getEvictedReqs := make([]*GetProtoSessionHitsRequest, 0, len(protosessionsForEviction))
	for clientID := range protosessionsForEviction {
		getEvictedReqs = append(getEvictedReqs, NewGetProtoSessionHitsRequest(clientID))
	}

	return appendReqs, markReqs, getEvictedReqs, nil
}

// checkHandleBatchResponses validates responses from the HandleBatch call.
func (o *Orchestrator) checkHandleBatchResponses(
	appendResps []*AppendHitsToProtoSessionResponse,
	markResps []*MarkProtoSessionClosingForGivenBucketResponse,
) *ProtosessionError {
	for _, response := range appendResps {
		if response.Err != nil {
			return NewProtosessionError(response.Err, true)
		}
	}
	for _, response := range markResps {
		if response.Err != nil {
			return NewProtosessionError(response.Err, true)
		}
	}
	return nil
}

// processEvictedProtosessions collects hits from evicted proto-sessions and re-queues them.
func (o *Orchestrator) processEvictedProtosessions(
	ctx context.Context,
	getResps []*GetProtoSessionHitsResponse,
	protosessionsForEviction map[hits.ClientID][]*hits.Hit,
) *ProtosessionError {
	for _, response := range getResps {
		if response.Err != nil {
			return NewProtosessionError(response.Err, true)
		}
		for _, hit := range response.Hits {
			protosessionsForEviction[hit.AuthoritativeClientID] = append(
				protosessionsForEviction[hit.AuthoritativeClientID], hit,
			)
		}
	}

	allHitsToBeRequeued := make([]*hits.Hit, 0)
	for _, evictedHits := range protosessionsForEviction {
		allHitsToBeRequeued = append(allHitsToBeRequeued, evictedHits...)
	}

	requeueStart := time.Now()
	err := o.requeuer.Push(allHitsToBeRequeued)
	o.requeueHist.Record(ctx, time.Since(requeueStart).Seconds())
	if err != nil {
		return NewProtosessionError(err, true)
	}
	return nil
}

// cleanupDroppedAndEvicted removes proto-session data for evicted sessions and metadata for dropped hits.
func (o *Orchestrator) cleanupDroppedAndEvicted(
	ctx context.Context,
	hitsToDrop map[hits.ClientID]*hits.Hit,
	protosessionsForEviction map[hits.ClientID][]*hits.Hit,
	registry properties.SettingsRegistry,
) *ProtosessionError {
	removeHitRequests := make([]*RemoveProtoSessionHitsRequest, 0, len(protosessionsForEviction))
	for id := range protosessionsForEviction {
		removeHitRequests = append(removeHitRequests, NewRemoveProtoSessionHitsRequest(id))
	}

	removeHitMetadataRequests := make([]*RemoveAllHitRelatedMetadataRequest, 0)
	for _, hit := range hitsToDrop {
		settings, err := registry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return NewProtosessionError(err, true)
		}
		removeHitMetadataRequests = append(
			removeHitMetadataRequests,
			GetRemoveHitRelatedMetadataRequests([]*hits.Hit{hit}, settings)...,
		)
	}

	cleanupStart := time.Now()
	removeResponses, removeMetadataResponses, _ := o.backend.Cleanup(
		ctx, removeHitRequests, removeHitMetadataRequests, nil,
	)
	o.cleanupHist.Record(ctx, time.Since(cleanupStart).Seconds())

	for _, resp := range removeResponses {
		if resp.Err != nil {
			return NewProtosessionError(resp.Err, true)
		}
	}
	for _, resp := range removeMetadataResponses {
		if resp.Err != nil {
			return NewProtosessionError(resp.Err, true)
		}
	}
	return nil
}

// processBucket is the callback invoked by the timing wheel when it's time to close a bucket.
// It handles the "expiry side" of proto-sessions - when enough time has passed, we know no more
// hits will arrive for these sessions, so we can finalize them.
func (o *Orchestrator) processBucket(
	ctx context.Context,
	passedBucketNumber int64,
) (BucketNextInstruction, error) {
	// We process bucket N-1 when timing wheel signals N, to be able to prefetch the next bucket (see below)
	bucketNumber := passedBucketNumber - 1
	processBucketStart := time.Now()
	defer func() {
		o.processBucketHist.Record(ctx, time.Since(processBucketStart).Seconds())
	}()

	o.recordAndWarnLag(ctx, bucketNumber)

	// Look-ahead prefetch: while processing bucket N, we fetch bucket N+1 in the background.
	// The prefillNextBuckets goroutine handles these requests asynchronously.
	// On first run, we bootstrap by requesting the current bucket synchronously.
	// Fire off the prefetch for next bucket - by the time we finish processing current bucket,
	// next bucket's data will likely be ready, hiding I/O latency during catch-up scenarios.
	o.triggerBucketPrefetch(bucketNumber)

	getProtosessionsStart := time.Now()
	// This blocks until the prefetch worker delivers the data (either freshly fetched
	// or already waiting from the previous iteration's look-ahead).
	responses := <-o.nextBucketResponses
	o.getProtosessionsHist.Record(ctx, time.Since(getProtosessionsStart).Seconds())

	if len(responses) == 0 {
		return BucketProcessingAdvance, nil
	}

	response := responses[0]
	if response.Err != nil {
		return BucketProcessingNoop, response.Err
	}

	// We collect all proto-sessions and their cleanup requests, then batch-process them.
	// The Closer receives sorted hit slices - ordering matters because session logic depends on
	// hit sequence (e.g., first hit determines session start, last hit determines session end).
	protoSessionsBatch, removeHitReqs, removeMetadataReqs, totalHits, err := o.buildProtoSessionsBatch(
		response.ProtoSessions,
	)
	if err != nil {
		return BucketProcessingNoop, err
	}

	// This is the moment proto-sessions become actual sessions. The Closer is responsible for
	// computing session-level aggregates from hits and publishing them to the warehouse.
	// After this point, the session data is persisted and proto-session state can be discarded.
	if err := o.closeBatch(ctx, protoSessionsBatch, totalHits); err != nil {
		return BucketProcessingNoop, err
	}

	// Cleanup phase: now that sessions are persisted, we remove all transient proto-session state.
	// This includes the hit data, identifier conflict metadata, and the bucket registration itself.
	if err := o.cleanupBucket(ctx, bucketNumber, removeHitReqs, removeMetadataReqs); err != nil {
		return BucketProcessingNoop, err
	}

	return BucketProcessingAdvance, nil
}

// recordAndWarnLag calculates processing lag and emits a warning if it exceeds threshold.
func (o *Orchestrator) recordAndWarnLag(ctx context.Context, bucketNumber int64) {
	lag := time.Duration(
		o.timingWheel.BucketNumber(time.Now().UTC())-bucketNumber,
	) * o.timingWheel.tickInterval
	if lag > time.Minute && time.Since(o.lastLagWarn) > time.Second {
		logrus.Warnf("Processing lag is high: %s, catching up...", lag)
		o.lastLagWarn = time.Now()
	}
	o.processingLagGauge.Record(ctx, lag.Seconds())
}

// triggerBucketPrefetch handles the look-ahead prefetch pattern for bucket data.
func (o *Orchestrator) triggerBucketPrefetch(bucketNumber int64) {
	if !o.processBucketAlreadyRan {
		o.processBucketAlreadyRan = true
		o.nextBucketRequests <- []*GetAllProtosessionsForBucketRequest{
			NewGetAllProtosessionsForBucketRequest(bucketNumber),
		}
	}
	go func() {
		o.nextBucketRequests <- []*GetAllProtosessionsForBucketRequest{
			NewGetAllProtosessionsForBucketRequest(bucketNumber + 1),
		}
	}()
}

// buildProtoSessionsBatch prepares sorted proto-sessions and cleanup requests from raw bucket data.
func (o *Orchestrator) buildProtoSessionsBatch(
	protoSessions [][]*hits.Hit,
) (
	[][]*hits.Hit,
	[]*RemoveProtoSessionHitsRequest,
	[]*RemoveAllHitRelatedMetadataRequest,
	int,
	error,
) {
	removeHitReqs := make([]*RemoveProtoSessionHitsRequest, 0, len(protoSessions))
	removeMetadataReqs := make([]*RemoveAllHitRelatedMetadataRequest, 0)
	batch := make([][]*hits.Hit, 0, len(protoSessions))
	totalHits := 0

	for _, protoSessionHits := range protoSessions {
		if len(protoSessionHits) == 0 {
			continue
		}

		sortedHits := sortHitsByServerReceivedTime(protoSessionHits)
		settings, err := o.settingsRegistry.GetByPropertyID(sortedHits[0].PropertyID)
		if err != nil {
			return nil, nil, nil, 0, err
		}

		for i, h := range sortedHits {
			if h == nil {
				logrus.Fatalf("hit is nil in processBucket at index %d", i)
			}
		}

		removeMetadataReqs = append(removeMetadataReqs, GetRemoveHitRelatedMetadataRequests(sortedHits, settings)...)
		batch = append(batch, sortedHits)
		totalHits += len(sortedHits)

		protoSessionID := sortedHits[0].AuthoritativeClientID
		removeHitReqs = append(removeHitReqs, NewRemoveProtoSessionHitsRequest(protoSessionID))
	}

	return batch, removeHitReqs, removeMetadataReqs, totalHits, nil
}

// closeBatch sends proto-sessions to the Closer for finalization into actual sessions.
func (o *Orchestrator) closeBatch(ctx context.Context, batch [][]*hits.Hit, totalHits int) error {
	if len(batch) == 0 {
		return nil
	}
	closeStart := time.Now()
	err := o.closer.Close(batch)
	o.closeHist.Record(ctx, time.Since(closeStart).Seconds())
	if err != nil {
		return err
	}
	o.hitsClosedCounter.Add(ctx, int64(totalHits))
	return nil
}

// cleanupBucket removes all transient proto-session state after sessions are persisted.
func (o *Orchestrator) cleanupBucket(
	ctx context.Context,
	bucketNumber int64,
	removeHitReqs []*RemoveProtoSessionHitsRequest,
	removeMetadataReqs []*RemoveAllHitRelatedMetadataRequest,
) error {
	cleanupStart := time.Now()
	removeResps, metadataResps, bucketResps := o.backend.Cleanup(
		ctx, removeHitReqs, removeMetadataReqs, []*RemoveBucketMetadataRequest{
			NewRemoveBucketMetadataRequest(bucketNumber),
		})
	o.cleanupHist.Record(ctx, time.Since(cleanupStart).Seconds())

	for _, resp := range removeResps {
		if resp.Err != nil {
			return resp.Err
		}
	}
	for _, resp := range metadataResps {
		if resp.Err != nil {
			return resp.Err
		}
	}
	for _, resp := range bucketResps {
		if resp.Err != nil {
			return resp.Err
		}
	}
	return nil
}

// prefillNextBuckets runs as a background goroutine, serving as the async I/O worker for
// processBucket's look-ahead pattern. It decouples bucket data fetching from processing,
// allowing the timing wheel to hide storage latency during catch-up.
func (o *Orchestrator) prefillNextBuckets() {
	for {
		select {
		case <-o.ctx.Done():
			logrus.Debugf("Prefill next buckets: context done")
			return
		case requests := <-o.nextBucketRequests:
			responses := o.backend.GetAllProtosessionsForBucket(o.ctx, requests)
			o.nextBucketResponses <- responses
		}
	}
}

func sortHitsByServerReceivedTime(hitsToSort []*hits.Hit) []*hits.Hit {
	if len(hitsToSort) <= 1 {
		return hitsToSort
	}

	sorted := make([]*hits.Hit, len(hitsToSort))
	copy(sorted, hitsToSort)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ServerReceivedTime.Before(sorted[j].ServerReceivedTime)
	})

	return sorted
}
