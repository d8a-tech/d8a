package protosessions

import (
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
)

// GetConflictCheckRequests returns a list of identifier conflict requests for a given hit and settings
func GetConflictCheckRequests(
	hit *hits.Hit,
	settings *properties.Settings,
	guard IdentifierIsolationGuard,
) []*IdentifierConflictRequest {
	requests := make([]*IdentifierConflictRequest, 0)
	if settings.SessionJoinBySessionStamp {
		requests = append(requests, NewIdentifierConflictRequest(
			hit,
			"session_stamp",
			func(h *hits.Hit) string {
				return guard.IsolatedSessionStamp(h)
			},
		))
	}
	if settings.SessionJoinByUserID && hit.UserID != nil {
		requests = append(requests, NewIdentifierConflictRequest(
			hit,
			"user_id",
			func(h *hits.Hit) string {
				stamp, err := guard.IsolatedUserID(h)
				if err != nil {
					// Fallback to raw UserID if stamp generation fails
					return *h.UserID
				}
				return stamp
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
	guard IdentifierIsolationGuard,
) []*RemoveAllHitRelatedMetadataRequest {
	if len(protoSession) == 0 {
		return nil
	}
	requests := make([]*RemoveAllHitRelatedMetadataRequest, 0)

	// Collect unique session stamps and map them to representative hits
	sessionStampToHit := make(map[string]*hits.Hit)
	if settings.SessionJoinBySessionStamp {
		for _, hit := range protoSession {
			sessionStamp := guard.IsolatedSessionStamp(hit)
			if _, exists := sessionStampToHit[sessionStamp]; !exists {
				sessionStampToHit[sessionStamp] = hit
			}
		}
		for sessionStamp, hit := range sessionStampToHit {
			requests = append(requests, NewRemoveAllHitRelatedMetadataRequest(
				hit,
				"session_stamp",
				func(h *hits.Hit) string {
					return sessionStamp
				},
			))
		}
	}

	// Collect unique user IDs and map them to representative hits
	userIDToHit := make(map[string]*hits.Hit)
	if settings.SessionJoinByUserID {
		for _, hit := range protoSession {
			if hit.UserID != nil {
				userIDStamp, err := guard.IsolatedUserID(hit)
				if err != nil {
					// Skip if stamp generation fails
					continue
				}
				if _, exists := userIDToHit[userIDStamp]; !exists {
					userIDToHit[userIDStamp] = hit
				}
			}
		}
		for userIDStamp, hit := range userIDToHit {
			requests = append(requests, NewRemoveAllHitRelatedMetadataRequest(
				hit,
				"user_id",
				func(h *hits.Hit) string {
					return userIDStamp
				},
			))
		}
	}

	return requests
}
