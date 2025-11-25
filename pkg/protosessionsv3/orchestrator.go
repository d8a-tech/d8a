package protosessionsv3

import (
	"context"
	"sort"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/sirupsen/logrus"
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
	lastHitTime      time.Time
}

func NewOrchestrator(
	ctx context.Context,
	backend BatchedIOBackend,
	tickerStateBackend TimingWheelStateBackend,
	closer Closer,
	requeuer receiver.Storage,
	settingsRegistry properties.SettingsRegistry,
) *Orchestrator {
	o := &Orchestrator{
		ctx:              ctx,
		backend:          backend,
		closer:           closer,
		requeuer:         requeuer,
		settingsRegistry: settingsRegistry,
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
	if len(hitsBatch) == 0 {
		return nil
	}
	batchSettingsRegistry := o.settingsRegistry
	var requests []*IdentifierConflictRequest
	for _, hit := range hitsBatch {
		settings, err := batchSettingsRegistry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return NewProtosessionError(err, true)
		}
		requests = append(requests, GetConflictCheckRequests(hit, settings)...)
	}
	conflictsByOriginalAuthoritativeClientID := make(map[hits.ClientID]*IdentifierConflictResponse)
	results := o.backend.GetIdentifierConflicts(ctx, requests)
	for _, result := range results {
		if result.HasConflict {
			conflictsByOriginalAuthoritativeClientID[result.Request.Hit.AuthoritativeClientID] = result
		}
	}
	protosessionsForEviction := make(map[hits.ClientID][]*hits.Hit)
	hitsToBeSaved := make([]*hits.Hit, 0)
	for _, hit := range hitsBatch {
		if conflict, ok := conflictsByOriginalAuthoritativeClientID[hit.AuthoritativeClientID]; ok {
			MarkForEviction(hit, conflict.ConflictsWith)
			if protosessionsForEviction[conflict.ConflictsWith] == nil {
				protosessionsForEviction[conflict.ConflictsWith] = make([]*hits.Hit, 0)
			}
			protosessionsForEviction[conflict.ConflictsWith] = append(protosessionsForEviction[conflict.ConflictsWith], hit)
		} else {
			hitsToBeSaved = append(hitsToBeSaved, hit)
		}
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
	appendHitsResps, getProtoSessionHitsResps, markProtoSessionClosingForGivenBucketResps := o.backend.HandleBatch(
		ctx,
		appendHitsRequests,
		getProtoSessionHitsRequests,
		markProtoSessionClosingForGivenBucketRequests,
	)
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
	err := o.requeuer.Push(allHitsToBeRequeued)
	if err != nil {
		return NewProtosessionError(err, true)
	}

	removeHitRequests := make([]*RemoveProtoSessionHitsRequest, 0)
	for id := range protosessionsForEviction {
		removeHitRequests = append(removeHitRequests, NewRemoveProtoSessionHitsRequest(id))
	}

	removeResponses, removeAllHitRelatedMetadataResponses, _ := o.backend.Cleanup(
		ctx,
		removeHitRequests,
		nil,
		nil,
	)
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

	o.updateLastHitTime(hitsBatch[len(hitsBatch)-1].ServerReceivedTime)
	return nil
}

func (o *Orchestrator) updateLastHitTime(time time.Time) {
	logrus.Infof("updateLastHitTime: %s", time)
	o.lastHitTime = time
}

// This is called by the timing wheel to process a bucket.
func (o *Orchestrator) processBucket(
	ctx context.Context,
	bucketNumber int64,
) (BucketNextInstruction, error) {
	responses := o.backend.GetAllProtosessionsForBucket(
		ctx,
		[]*GetAllProtosessionsForBucketRequest{
			NewGetAllProtosessionsForBucketRequest(bucketNumber),
		},
	)

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
		err = o.closer.Close(sortedHits)
		if err != nil {
			return BucketProcessingNoop, err
		}

		protoSessionID := sortedHits[0].AuthoritativeClientID
		removeHitRequests = append(removeHitRequests, NewRemoveProtoSessionHitsRequest(protoSessionID))
	}

	removeResponses, removeAllHitRelatedMetadataResponses, removeBucketMetadataResponses := o.backend.Cleanup(
		ctx, removeHitRequests, removeAllHitRelatedMetadataRequests, []*RemoveBucketMetadataRequest{
			NewRemoveBucketMetadataRequest(bucketNumber),
		})
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
