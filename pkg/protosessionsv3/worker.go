package protosessionsv3

import (
	"context"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/pings"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
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
	tickerStateBackend TimingWheelStateBackend,
	closer Closer,
	requeuer receiver.Storage,
	settingsRegistry properties.SettingsRegistry,
) func(_ map[string]string, h *hits.HitProcessingTask) *worker.Error {
	orchestrator := NewOrchestrator(
		ctx,
		backend,
		tickerStateBackend,
		closer,
		requeuer,
		settingsRegistry,
	)
	return func(md map[string]string, h *hits.HitProcessingTask) *worker.Error {
		isPing, pingTimestamp := pings.IsTaskAPing(md)
		if isPing {
			// TODO: those ping timestamps bork the logic:
			/*
				INFO[0063] updateLastHitTime: 2025-11-25 23:20:46 +0100 CET
				INFO[0065] updateLastHitTime: 2025-11-25 23:20:47 +0100 CET
				INFO[0066] updateLastHitTime: 2025-11-25 23:20:48 +0100 CET
				INFO[0067] updateLastHitTime: 2025-11-25 23:20:48 +0100 CET
				INFO[0068] updateLastHitTime: 2025-11-25 23:20:49 +0100 CET
				INFO[0070] updateLastHitTime: 2025-11-25 23:20:49 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:50 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:37 +0100 CET
				INFO[0071] ping timestamp: 2025-11-25 23:20:53 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:53 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:34 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:39 +0100 CET
				INFO[0071] ping timestamp: 2025-11-25 23:20:55 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:55 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:30 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:39 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:38 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:35 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:42 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:40 +0100 CET
				INFO[0071] ping timestamp: 2025-11-25 23:21:01 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:21:01 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:20:41 +0100 CET
				INFO[0071] ping timestamp: 2025-11-25 23:21:02 +0100 CET
				INFO[0071] updateLastHitTime: 2025-11-25 23:21:02 +0100 CET

			*/
			logrus.Infof("ping timestamp: %s", pingTimestamp)
			orchestrator.updateLastHitTime(pingTimestamp)
			return nil
		}
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
