package protosessionsv3

import (
	"context"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/storage"
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
func Handler(
	ctx context.Context,
	set storage.Set,
	kv storage.KV,
	settingsRegistry properties.SettingsRegistry,
	encoder encoding.EncoderFunc,
	decoder encoding.DecoderFunc,
) func(_ map[string]string, h *hits.HitProcessingTask) *worker.Error {

	backend := NewNaiveGenericStorageBatchedIOBackend(kv, set, encoder, decoder)

	// TODO: consider adding local cache for parameter conflicts
	// TODO: add config
	return func(md map[string]string, h *hits.HitProcessingTask) *worker.Error {
		batchSettingsRegistry := settingsRegistry
		var requests []*IdentifierConflictRequest
		for _, hit := range h.Hits {
			settings, err := batchSettingsRegistry.GetByPropertyID(hit.PropertyID)
			if err != nil {
				return worker.NewError(worker.ErrTypeDroppable, err)
			}
			// TODO: What if both are conflicting against different sessions?
			if settings.SessionJoinBySessionStamp {
				requests = append(requests, &IdentifierConflictRequest{
					IdentifierType: "session_stamp",
					Hit:            hit,
					ExtractIdentifier: func(h *hits.Hit) string {
						return h.SessionStamp()
					},
				})
			}
			if settings.SessionJoinByUserID && hit.UserID != nil {
				requests = append(requests, &IdentifierConflictRequest{
					IdentifierType: "user_id",
					Hit:            hit,
					ExtractIdentifier: func(h *hits.Hit) string {
						return *h.UserID
					},
				})
			}
		}
		conflictsByOriginalAuthoritativeClientID := make(map[hits.ClientID]*IdentifierConflictResponse)
		results := backend.GetIdentifierConflicts(ctx, requests)
		for _, result := range results {
			if result.HasConflict {
				conflictsByOriginalAuthoritativeClientID[result.Request.Hit.AuthoritativeClientID] = result
			}
		}
		var forEvictionProtosessions map[hits.ClientID][]*hits.Hit
		var forSavingHits []*hits.Hit
		for _, hit := range h.Hits {
			if conflict, ok := conflictsByOriginalAuthoritativeClientID[hit.AuthoritativeClientID]; ok {
				MarkForEviction(hit, conflict.ConflictsWith)
				if forEvictionProtosessions[conflict.ConflictsWith] == nil {
					forEvictionProtosessions[conflict.ConflictsWith] = make([]*hits.Hit, 0)
				}
				forEvictionProtosessions[conflict.ConflictsWith] = append(forEvictionProtosessions[conflict.ConflictsWith], hit)
			} else {
				forSavingHits = append(forSavingHits, hit)
			}
		}

		return nil
	}
}

// TODO: Reminder for writing test cases - what if a hit was evicted multiple times? For exmaple user logged in from 3 machines
// and the protosessions were joined together?
