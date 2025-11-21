package protosessionsv3

import (
	"context"

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
	orchestrator := NewOrchestrator(backend, requeuer, settingsRegistry)
	return func(_ map[string]string, h *hits.HitProcessingTask) *worker.Error {
		err := orchestrator.Orchestrate(ctx, h.Hits)
		if err != nil {
			var errType worker.ErrorType
			if err.IsFatal() {
				errType = worker.ErrTypeRetryable
			} else {
				errType = worker.ErrTypeDroppable
			}
			return worker.NewError(errType, err)
		}
		return nil
	}

}
