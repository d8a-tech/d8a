package protosessions

import (
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/sirupsen/logrus"
)

// GetConflictCheckRequests returns a list of identifier conflict requests for a given hit and settings
func GetConflictCheckRequests(
	hit *hits.Hit,
	settings *properties.Settings,
) []*IdentifierConflictRequest {
	requests := make([]*IdentifierConflictRequest, 0)
	if settings.SessionJoinBySessionStamp {
		stamp, ok := GetIsolatedSessionStamp(hit)
		if !ok {
			logrus.Errorf("missing isolated session stamp metadata for hit %s, skipping conflict check", hit.ID)
		} else {
			requests = append(requests, NewIdentifierConflictRequest(
				hit,
				"session_stamp",
				func(h *hits.Hit) string {
					return stamp
				},
			))
		}
	}
	if settings.SessionJoinByUserID && hit.UserID != nil {
		stamp, ok := GetIsolatedUserIDStamp(hit)
		if !ok {
			logrus.Errorf("missing isolated user ID stamp metadata for hit %s, skipping conflict check", hit.ID)
		} else {
			requests = append(requests, NewIdentifierConflictRequest(
				hit,
				"user_id",
				func(h *hits.Hit) string {
					return stamp
				},
			))
		}
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
	requests := make([]*RemoveAllHitRelatedMetadataRequest, 0)

	// Collect unique session stamps and map them to representative hits
	sessionStampToHit := make(map[string]*hits.Hit)
	if settings.SessionJoinBySessionStamp {
		for _, hit := range protoSession {
			sessionStamp, ok := GetIsolatedSessionStamp(hit)
			if !ok {
				logrus.Errorf("missing isolated session stamp metadata for hit %s, skipping metadata removal", hit.ID)
				continue
			}
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
				userIDStamp, ok := GetIsolatedUserIDStamp(hit)
				if !ok {
					logrus.Errorf("missing isolated user ID stamp metadata for hit %s, skipping metadata removal", hit.ID)
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
