package protosessionsv3

import (
	"context"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/hits"
)

type EvicterMiddleware struct{}

func NewEvicterMiddleware() Middleware {
	return &EvicterMiddleware{}
}

func (m *EvicterMiddleware) Handle(ctx context.Context, allHits []*hits.Hit) error {
	backend := CtxMustValue[BatchedIOBackend](ctx, "backend")
	var requests []*IdentifierConflictRequest
	for _, hit := range allHits {
		requests = append(requests, &IdentifierConflictRequest{
			IdentifierType: "session_stamp",
			Hit:            hit,
			ExtractIdentifier: func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		})
	}
	results := backend.GetIdentifierConflicts(ctx, requests)
	for _, result := range results {
		if result.HasConflict {
			return fmt.Errorf("conflict with %s", result.ConflictsWith)
		}
	}
	return nil
}
