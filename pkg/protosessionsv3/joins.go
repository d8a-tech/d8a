package protosessionsv3

import (
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
)

// TODO: What if both are conflicting against different sessions?

func GetConflictCheckRequests(hit *hits.Hit, settings properties.Settings) []*IdentifierConflictRequest {
	requests := make([]*IdentifierConflictRequest, 0)
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
	return requests
}
