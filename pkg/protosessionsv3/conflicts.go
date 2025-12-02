package protosessionsv3

import (
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
)

// GetConflictCheckRequests returns a list of identifier conflict requests for a given hit and settings
func GetConflictCheckRequests(hit *hits.Hit, settings *properties.Settings) []*IdentifierConflictRequest {
	requests := make([]*IdentifierConflictRequest, 0)
	if settings.SessionJoinBySessionStamp {
		requests = append(requests, NewIdentifierConflictRequest(
			hit,
			"session_stamp",
			func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		))
	}
	if settings.SessionJoinByUserID && hit.UserID != nil {
		requests = append(requests, NewIdentifierConflictRequest(
			hit,
			"user_id",
			func(h *hits.Hit) string {
				return *h.UserID
			},
		))
	}
	return requests
}

// GetRemoveHitRelatedMetadataRequests returns a list of remove all
// hit related metadata requests for a given proto session and settings
func GetRemoveHitRelatedMetadataRequests(
	protoSession []*hits.Hit,
	settings *properties.Settings,
) []*RemoveAllHitRelatedMetadataRequest {
	if len(protoSession) == 0 {
		return nil
	}
	hit := protoSession[0]
	requests := make([]*RemoveAllHitRelatedMetadataRequest, 0)
	if settings.SessionJoinBySessionStamp {
		requests = append(requests, NewRemoveAllHitRelatedMetadataRequest(
			hit,
			"session_stamp",
			func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		))
	}
	if settings.SessionJoinByUserID && hit.UserID != nil {
		requests = append(requests, NewRemoveAllHitRelatedMetadataRequest(
			hit,
			"user_id",
			func(h *hits.Hit) string {
				return *h.UserID
			},
		))
	}
	return requests
}
