package protosessions

import (
	"fmt"
)

// DoTick is an e2e test utility function that performs a single tick of the closeTriggerMiddleware.
func DoTick(closeTriggerCandidate Middleware) {
	closeTrigger, ok := closeTriggerCandidate.(*closeTriggerMiddleware)
	if !ok {
		panic(fmt.Errorf("closeTriggerCandidate is not a closeTriggerMiddleware"))
	}
	err := closeTrigger.tick()
	if err != nil {
		panic(fmt.Errorf("failed to tick closeTriggerMiddleware: %w", err))
	}
}

// StopCloseTrigger is an e2e test utility function that stops the closeTriggerMiddleware.
func StopCloseTrigger(closeTriggerCandidate Middleware) {
	closeTrigger, ok := closeTriggerCandidate.(*closeTriggerMiddleware)
	if !ok {
		panic(fmt.Errorf("closeTriggerCandidate is not a closeTriggerMiddleware"))
	}
	closeTrigger.doStop()
}
