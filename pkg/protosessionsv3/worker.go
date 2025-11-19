package protosessionsv3

import (
	"context"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
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
	encoder encoding.EncoderFunc,
	decoder encoding.DecoderFunc,
	middlewares []Middleware,
) func(_ map[string]string, h *hits.HitProcessingTask) *worker.Error {

	ctx = CtxSetValue(ctx, "backend", NewNaiveGenericStorageBatchedIOBackend(kv, set, encoder, decoder))

	return func(md map[string]string, h *hits.HitProcessingTask) *worker.Error {
		for _, middleware := range middlewares {
			err := middleware.Handle(ctx, h.Hits)
			if err != nil {
				return worker.NewError(worker.ErrTypeDroppable, err)
			}
		}

		return nil
	}
}
