package protosessionsv3

import (
	"context"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/receiver"
)

type Orchestrator struct {
	backend          BatchedIOBackend
	requeuer         receiver.Storage
	settingsRegistry properties.SettingsRegistry
}

func NewOrchestrator(backend BatchedIOBackend, requeuer receiver.Storage, settingsRegistry properties.SettingsRegistry) *Orchestrator {
	return &Orchestrator{
		backend:          backend,
		requeuer:         requeuer,
		settingsRegistry: settingsRegistry,
	}
}

func (o *Orchestrator) Orchestrate(ctx context.Context, hitsBatch []*hits.Hit) *ProtosessionError {
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
		getProtoSessionHitsRequests = append(getProtoSessionHitsRequests, &GetProtoSessionHitsRequest{
			ProtoSessionID: clientID,
		})
	}
	for _, hit := range hitsToBeSaved {
		settings, err := batchSettingsRegistry.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return NewProtosessionError(err, true)
		}
		markProtoSessionClosingForGivenBucketRequests = append(
			markProtoSessionClosingForGivenBucketRequests,
			&MarkProtoSessionClosingForGivenBucketRequest{
				ProtoSessionID: hit.AuthoritativeClientID,
				BucketID:       BucketNumber(hit.ServerReceivedTime.Add(settings.SessionDuration), 1*time.Second),
			},
		)
		appendHitsRequests = append(appendHitsRequests, &AppendHitsToProtoSessionRequest{
			ProtoSessionID: hit.AuthoritativeClientID,
			Hits:           []*hits.Hit{hit},
		})
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
	return nil
}
