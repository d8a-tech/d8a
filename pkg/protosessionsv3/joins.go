package protosessionsv3

import (
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/sirupsen/logrus"
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

func GetRemoveHitRelatedMetadataRequests(protoSession []*hits.Hit, settings properties.Settings) []*RemoveAllHitRelatedMetadataRequest {
	if len(protoSession) == 0 {
		return nil
	}
	hit := protoSession[0]
	requests := make([]*RemoveAllHitRelatedMetadataRequest, 0)
	if settings.SessionJoinBySessionStamp {
		requests = append(requests, &RemoveAllHitRelatedMetadataRequest{
			IdentifierType: "session_stamp",
			ExtractIdentifier: func(h *hits.Hit) string {
				if h == nil {
					//TODO: Debugging
					logrus.Fatalf("hit is nil in GetRemoveHitRelatedMetadataRequests")
				}
				return h.SessionStamp()
			},
		})
	}
	if settings.SessionJoinByUserID && hit.UserID != nil {
		requests = append(requests, &RemoveAllHitRelatedMetadataRequest{
			IdentifierType: "user_id",
			ExtractIdentifier: func(h *hits.Hit) string {
				return *h.UserID
			},
		})
	}
	return requests
}

func GetRemoveHitRelatedMetadataRequestsForEviction(protoSession []*hits.Hit, settings properties.Settings) []*RemoveAllHitRelatedMetadataRequest {
	if len(protoSession) == 0 {
		return nil
	}
	hit := protoSession[0]
	requests := make([]*RemoveAllHitRelatedMetadataRequest, 0)
	if settings.SessionJoinBySessionStamp {
		requests = append(requests, &RemoveAllHitRelatedMetadataRequest{
			hit:            hit,
			IdentifierType: "session_stamp",
			ExtractIdentifier: func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		})
	}
	if settings.SessionJoinByUserID && hit.UserID != nil {
		requests = append(requests, &RemoveAllHitRelatedMetadataRequest{
			hit:            hit,
			IdentifierType: "user_id",
			ExtractIdentifier: func(h *hits.Hit) string {
				return *h.UserID
			},
		})
	}
	return requests
}
