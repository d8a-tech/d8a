package newprotosessions

import (
	"context"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/worker"
)

// ==================== Chain Function ====================

// Chain chains middlewares with a storage backend
func Chain(middlewares []BatchMiddleware, backend StorageBackend) func(context.Context, *HitBatch) error {
	return func(ctx context.Context, batch *HitBatch) error {
		var handle func(idx int) error

		handle = func(idx int) error {
			if idx >= len(middlewares) {
				return nil
			}

			return middlewares[idx].Handle(ctx, batch, func(ops []IOOperation) (IOResults, error) {
				// Execute IO operations
				results, err := backend.ExecuteBatch(ctx, ops)
				if err != nil {
					return nil, err
				}

				// Continue to next middleware
				err = handle(idx + 1)
				if err != nil {
					return nil, err
				}

				return results, nil
			})
		}

		return handle(0)
	}
}

// ==================== Handler Function ====================

// NewBatchHandler creates a new batch handler for hit processing
func NewBatchHandler(
	ctx context.Context,
	backend StorageBackend,
	middlewares []BatchMiddleware,
) func(map[string]string, *hits.HitProcessingTask) *worker.Error {
	handler := Chain(middlewares, backend)

	return func(_ map[string]string, task *hits.HitProcessingTask) *worker.Error {
		batch := &HitBatch{Hits: task.Hits}
		err := handler(ctx, batch)
		if err != nil {
			return worker.NewError(worker.ErrTypeDroppable, err)
		}
		return nil
	}
}
