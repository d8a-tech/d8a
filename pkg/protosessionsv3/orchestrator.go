package protosessionsv3

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/monitoring"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Closer defines an interface for closing and processing hit sessions
type Closer interface {
	Close(protosession []*hits.Hit) error
}

type Orchestrator struct {
	ctx              context.Context
	backend          BatchedIOBackend
	closer           Closer
	requeuer         receiver.Storage
	settingsRegistry properties.SettingsRegistry
	timingWheel      *TimingWheel
	evictionStrategy EvictionStrategy
	lastHitTime      time.Time
	lock             sync.Mutex

	orchestrateHist      metric.Float64Histogram
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
	meter := otel.GetMeterProvider().Meter("protosessions")

	orchestrateHist, _ := meter.Float64Histogram(
		"protosessions.orchestrate.duration",
		metric.WithDescription("Full orchestration duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	conflictCheckHist, _ := meter.Float64Histogram(
		"protosessions.conflict_check.duration",
		metric.WithDescription("Conflict detection duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	handleBatchHist, _ := meter.Float64Histogram(
		"protosessions.handle_batch.duration",
		metric.WithDescription("Batch handling duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	cleanupHist, _ := meter.Float64Histogram(
		"protosessions.cleanup.duration",
		metric.WithDescription("Cleanup operations duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	processBucketHist, _ := meter.Float64Histogram(
		"protosessions.process_bucket.duration",
		metric.WithDescription("Bucket processing duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	getProtosessionsHist, _ := meter.Float64Histogram(
		"protosessions.get_protosessions.duration",
		metric.WithDescription("Fetching protosessions for bucket"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	closeHist, _ := meter.Float64Histogram(
		"protosessions.close.duration",
		metric.WithDescription("Closing proto-session duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	requeueHist, _ := meter.Float64Histogram(
		"protosessions.requeue.duration",
		metric.WithDescription("Requeuing hits duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	hitsReceivedCounter, _ := meter.Int64Counter(
		"protosessions.hits.received",
		metric.WithDescription("Hits received for live processing"),
	)
	hitsClosedCounter, _ := meter.Int64Counter(
		"protosessions.hits.closed",
		metric.WithDescription("Hits closed when bucket expires"),
	)
	evictionsCounter, _ := meter.Int64Counter(
		"protosessions.evictions",
		metric.WithDescription("Evictions triggered"),
	)
	processingLagGauge, _ := meter.Float64Gauge(
		"protosessions.processing.lag",
		metric.WithDescription("Processing lag in seconds"),
		metric.WithUnit("s"),
	)

	o := &Orchestrator{
		ctx:                  ctx,
		backend:              backend,
		closer:               closer,
		requeuer:             requeuer,
		settingsRegistry:     settingsRegistry,
		evictionStrategy:     evictionStrategy,
		orchestrateHist:      orchestrateHist,
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
		func() time.Time { return o.lastHitTime },
	)
	go o.timingWheel.Start(ctx)
	return o
}

func (o *Orchestrator) Orchestrate(
	ctx context.Context,
	hitsBatch []*hits.Hit,
) *ProtosessionError {
	orchestrateStart := time.Now()
	defer func() {
		o.orchestrateHist.Record(ctx, time.Since(orchestrateStart).Seconds())
	}()

	if len(hitsBatch) == 0 {
		return nil
	}
	batchSettingsRegistry := o.settingsRegistry
	newBatch := []*hits.Hit{}
	uniqueBuckets := make(map[int64][]*hits.Hit)
	for _, hit := range hitsBatch {
		settings, err := batchSettingsRegistry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return NewProtosessionError(err, true)
		}
		bucketNumber := o.timingWheel.BucketNumber(hit.ServerReceivedTime.Add(settings.SessionDuration))
		if _, ok := uniqueBuckets[bucketNumber]; !ok {
			uniqueBuckets[bucketNumber] = make([]*hits.Hit, 0)
		}
		uniqueBuckets[bucketNumber] = append(uniqueBuckets[bucketNumber], hit)
	}
	hitsToDrop := map[hits.ClientID]*hits.Hit{}
	for bucketNumber, hits := range uniqueBuckets {
		ok := o.timingWheel.lock.TryLock(bucketNumber)
		if !ok {
			logrus.Warnf("Dropping %d hits for bucket %d because it is being processed", len(hits), bucketNumber)
			for _, hit := range hits {
				hitsToDrop[hit.AuthoritativeClientID] = hit
			}
			continue
		}
		defer o.timingWheel.lock.Drop(bucketNumber)
		if bucketNumber <= o.timingWheel.BucketNumber(o.timingWheel.getCurrentTime()) {
			logrus.Warnf("Dropping %d hits for bucket %d because it is already expired (current bucket: %d)", len(hits), bucketNumber, o.timingWheel.BucketNumber(o.timingWheel.getCurrentTime()))
			for _, hit := range hits {
				hitsToDrop[hit.AuthoritativeClientID] = hit
			}
			continue
		}
		newBatch = append(newBatch, hits...)
	}
	var requests []*IdentifierConflictRequest
	for _, hit := range newBatch {
		settings, err := batchSettingsRegistry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return NewProtosessionError(err, true)
		}
		requests = append(requests, GetConflictCheckRequests(hit, settings)...)
	}
	conflictsByOriginalAuthoritativeClientID := make(map[hits.ClientID]*IdentifierConflictResponse)
	conflictCheckStart := time.Now()
	results := o.backend.GetIdentifierConflicts(ctx, requests)
	o.conflictCheckHist.Record(ctx, time.Since(conflictCheckStart).Seconds())
	for _, result := range results {
		if result.HasConflict {
			conflictsByOriginalAuthoritativeClientID[result.Request.Hit.AuthoritativeClientID] = result
		}
	}
	protosessionsForEviction := make(map[hits.ClientID][]*hits.Hit)
	hitsToBeSaved := make([]*hits.Hit, 0)
	evictionCount := int64(0)
	for _, hit := range newBatch {
		if conflict, ok := conflictsByOriginalAuthoritativeClientID[hit.AuthoritativeClientID]; ok {
			o.evictionStrategy(hit, conflict, &hitsToBeSaved, protosessionsForEviction)
			evictionCount++
		} else {
			hitsToBeSaved = append(hitsToBeSaved, hit)
		}
	}
	if evictionCount > 0 {
		o.evictionsCounter.Add(ctx, evictionCount)
	}
	appendHitsRequests := make([]*AppendHitsToProtoSessionRequest, 0)
	markProtoSessionClosingForGivenBucketRequests := make([]*MarkProtoSessionClosingForGivenBucketRequest, 0)
	getProtoSessionHitsRequests := make([]*GetProtoSessionHitsRequest, 0)
	for clientID := range protosessionsForEviction {
		getProtoSessionHitsRequests = append(getProtoSessionHitsRequests, NewGetProtoSessionHitsRequest(clientID))
	}
	for _, hit := range hitsToBeSaved {
		settings, err := batchSettingsRegistry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return NewProtosessionError(err, true)
		}
		markProtoSessionClosingForGivenBucketRequests = append(
			markProtoSessionClosingForGivenBucketRequests,
			NewMarkProtoSessionClosingForGivenBucketRequest(
				hit.AuthoritativeClientID,
				o.timingWheel.BucketNumber(hit.ServerReceivedTime.Add(settings.SessionDuration)),
			),
		)
		appendHitsRequests = append(appendHitsRequests, NewAppendHitsToProtoSessionRequest(
			hit.AuthoritativeClientID,
			[]*hits.Hit{hit},
		))
	}
	handleBatchStart := time.Now()
	appendHitsResps, getProtoSessionHitsResps, markProtoSessionClosingForGivenBucketResps := o.backend.HandleBatch(
		ctx,
		appendHitsRequests,
		getProtoSessionHitsRequests,
		markProtoSessionClosingForGivenBucketRequests,
	)
	o.handleBatchHist.Record(ctx, time.Since(handleBatchStart).Seconds())
	for _, response := range appendHitsResps {
		if response.Err != nil {
			return NewProtosessionError(response.Err, true)
		}
	}
	for _, response := range markProtoSessionClosingForGivenBucketResps {
		if response.Err != nil {
			return NewProtosessionError(response.Err, true)
		}
	}
	for _, response := range getProtoSessionHitsResps {
		if response.Err != nil {
			return NewProtosessionError(response.Err, true)
		}
		for _, hit := range response.Hits {
			theList, ok := protosessionsForEviction[hit.AuthoritativeClientID]
			if !ok {
				theList = make([]*hits.Hit, 0)
			}
			theList = append(theList, hit)
			protosessionsForEviction[hit.AuthoritativeClientID] = theList
		}
	}

	allHitsToBeRequeued := make([]*hits.Hit, 0)
	for _, hits := range protosessionsForEviction {
		allHitsToBeRequeued = append(allHitsToBeRequeued, hits...)
	}
	requeueStart := time.Now()
	err := o.requeuer.Push(allHitsToBeRequeued)
	o.requeueHist.Record(ctx, time.Since(requeueStart).Seconds())
	if err != nil {
		return NewProtosessionError(err, true)
	}

	removeHitRequests := make([]*RemoveProtoSessionHitsRequest, 0)
	for id := range protosessionsForEviction {
		logrus.Errorf("removing hit for id %s", id)
		removeHitRequests = append(removeHitRequests, NewRemoveProtoSessionHitsRequest(id))
	}
	removeHitMetadataRequests := make([]*RemoveAllHitRelatedMetadataRequest, 0)
	for _, hit := range hitsToDrop {
		settings, err := batchSettingsRegistry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return NewProtosessionError(err, true)
		}
		removeHitMetadataRequests = append(
			removeHitMetadataRequests,
			GetRemoveHitRelatedMetadataRequests([]*hits.Hit{hit}, settings)...,
		)

	}

	cleanupStart := time.Now()
	removeResponses, removeAllHitRelatedMetadataResponses, _ := o.backend.Cleanup(
		ctx,
		removeHitRequests,
		removeHitMetadataRequests,
		nil,
	)
	o.cleanupHist.Record(ctx, time.Since(cleanupStart).Seconds())
	for _, removeResponse := range removeResponses {
		if removeResponse.Err != nil {
			return NewProtosessionError(removeResponse.Err, true)
		}
	}
	for _, removeAllHitRelatedMetadataResponse := range removeAllHitRelatedMetadataResponses {
		if removeAllHitRelatedMetadataResponse.Err != nil {
			return NewProtosessionError(removeAllHitRelatedMetadataResponse.Err, true)
		}
	}
	if len(newBatch) > 0 {
		o.updateLastHitTime(newBatch[len(newBatch)-1].ServerReceivedTime)
		o.hitsReceivedCounter.Add(ctx, int64(len(newBatch)))
	}
	return nil
}

func (o *Orchestrator) updateLastHitTime(theTime time.Time) {
	if theTime.After(o.lastHitTime) {
		o.lastHitTime = theTime
	}
}

// This is called by the timing wheel to process a bucket.
func (o *Orchestrator) processBucket(
	ctx context.Context,
	bucketNumber int64,
) (BucketNextInstruction, error) {
	processBucketStart := time.Now()
	defer func() {
		o.processBucketHist.Record(ctx, time.Since(processBucketStart).Seconds())
	}()

	lagSeconds := float64(
		o.timingWheel.BucketNumber(time.Now().UTC())-bucketNumber,
	) * o.timingWheel.tickInterval.Seconds()
	o.processingLagGauge.Record(ctx, lagSeconds)

	getProtosessionsStart := time.Now()
	responses := o.backend.GetAllProtosessionsForBucket(
		ctx,
		[]*GetAllProtosessionsForBucketRequest{
			NewGetAllProtosessionsForBucketRequest(bucketNumber),
		},
	)
	o.getProtosessionsHist.Record(ctx, time.Since(getProtosessionsStart).Seconds())

	if len(responses) == 0 {
		return BucketProcessingAdvance, nil
	}

	response := responses[0]
	if response.Err != nil {
		return BucketProcessingNoop, response.Err
	}

	removeHitRequests := make([]*RemoveProtoSessionHitsRequest, 0)
	removeAllHitRelatedMetadataRequests := make([]*RemoveAllHitRelatedMetadataRequest, 0)

	for _, protoSessionHits := range response.ProtoSessions {
		if len(protoSessionHits) == 0 {
			continue
		}

		sortedHits := sortHitsByServerReceivedTime(protoSessionHits)
		settings, err := o.settingsRegistry.GetByPropertyID(sortedHits[0].PropertyID)
		if err != nil {
			return BucketProcessingNoop, err
		}
		for i, h := range sortedHits {
			if h == nil {
				logrus.Fatalf("hit is nil in processBucket at index %d", i)
			}
		}
		removeAllHitRelatedMetadataRequests = append(
			removeAllHitRelatedMetadataRequests,
			GetRemoveHitRelatedMetadataRequests(sortedHits, settings)...,
		)

		// Close the proto-session
		closeStart := time.Now()
		err = o.closer.Close(sortedHits)
		o.closeHist.Record(ctx, time.Since(closeStart).Seconds())
		if err != nil {
			return BucketProcessingNoop, err
		}
		o.hitsClosedCounter.Add(ctx, int64(len(sortedHits)))

		protoSessionID := sortedHits[0].AuthoritativeClientID
		removeHitRequests = append(removeHitRequests, NewRemoveProtoSessionHitsRequest(protoSessionID))
	}

	cleanupStart := time.Now()
	removeResponses, removeAllHitRelatedMetadataResponses, removeBucketMetadataResponses := o.backend.Cleanup(
		ctx, removeHitRequests, removeAllHitRelatedMetadataRequests, []*RemoveBucketMetadataRequest{
			NewRemoveBucketMetadataRequest(bucketNumber),
		})
	o.cleanupHist.Record(ctx, time.Since(cleanupStart).Seconds())
	for _, removeResponse := range removeResponses {
		if removeResponse.Err != nil {
			return BucketProcessingNoop, removeResponse.Err
		}
	}
	for _, removeAllHitRelatedMetadataResponse := range removeAllHitRelatedMetadataResponses {
		if removeAllHitRelatedMetadataResponse.Err != nil {
			return BucketProcessingNoop, removeAllHitRelatedMetadataResponse.Err
		}
	}
	for _, removeBucketMetadataResponse := range removeBucketMetadataResponses {
		if removeBucketMetadataResponse.Err != nil {
			// We advance - if bucket deletion fails TODO
			return BucketProcessingNoop, removeBucketMetadataResponse.Err
		}
	}

	return BucketProcessingAdvance, nil
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
