package protosessionsv3

import (
	"context"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/worker"
)

// Middleware defines an interface for task processing middleware
type Middleware interface {
	Handle(
		ctx context.Context,
		hits []*hits.Hit,
	) error
}

// Handler returns a function that processes hit processing tasks.
// TODO: Rethink each "Droppable" error
func Handler(
	ctx context.Context,
	backend BatchedIOBackend,
	requeuer receiver.Storage,
	settingsRegistry properties.SettingsRegistry,
) func(_ map[string]string, h *hits.HitProcessingTask) *worker.Error {

	return func(md map[string]string, h *hits.HitProcessingTask) *worker.Error {
		batchSettingsRegistry := settingsRegistry
		var requests []*IdentifierConflictRequest
		for _, hit := range h.Hits {
			settings, err := batchSettingsRegistry.GetByPropertyID(hit.PropertyID)
			if err != nil {
				return worker.NewError(worker.ErrTypeDroppable, err)
			}
			requests = append(requests, GetConflictCheckRequests(hit, settings)...)
		}
		conflictsByOriginalAuthoritativeClientID := make(map[hits.ClientID]*IdentifierConflictResponse)
		results := backend.GetIdentifierConflicts(ctx, requests)
		for _, result := range results {
			if result.HasConflict {
				conflictsByOriginalAuthoritativeClientID[result.Request.Hit.AuthoritativeClientID] = result
			}
		}
		protosessionsForEviction := make(map[hits.ClientID][]*hits.Hit)
		hitsToBeSaved := make([]*hits.Hit, 0)
		for _, hit := range h.Hits {
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
		var appendHitsRequests []*AppendHitsToProtoSessionRequest
		var markProtoSessionClosingForGivenBucketRequests []*MarkProtoSessionClosingForGivenBucketRequest
		var getProtoSessionHitsRequests []*GetProtoSessionHitsRequest
		for clientID := range protosessionsForEviction {
			getProtoSessionHitsRequests = append(getProtoSessionHitsRequests, &GetProtoSessionHitsRequest{
				ProtoSessionID: clientID,
			})
		}
		for _, hit := range hitsToBeSaved {
			settings, err := batchSettingsRegistry.GetByPropertyID(hit.PropertyID)
			if err != nil {
				return worker.NewError(worker.ErrTypeDroppable, err)
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
		appendHitsResps, getProtoSessionHitsResps, markProtoSessionClosingForGivenBucketResps := backend.HandleBatch(
			ctx,
			appendHitsRequests,
			getProtoSessionHitsRequests,
			markProtoSessionClosingForGivenBucketRequests,
		)
		for _, response := range appendHitsResps {
			if response.Err != nil {
				return worker.NewError(worker.ErrTypeDroppable, response.Err)
			}
		}
		for _, response := range markProtoSessionClosingForGivenBucketResps {
			if response.Err != nil {
				return worker.NewError(worker.ErrTypeDroppable, response.Err)
			}
		}
		for _, response := range getProtoSessionHitsResps {
			if response.Err != nil {
				return worker.NewError(worker.ErrTypeDroppable, response.Err)
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
		err := requeuer.Push(allHitsToBeRequeued)
		if err != nil {
			return worker.NewError(worker.ErrTypeDroppable, err)
		}
		return nil
	}
}
