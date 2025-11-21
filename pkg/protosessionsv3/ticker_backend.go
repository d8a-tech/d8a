package protosessionsv3

import (
	"context"
)

// TickerStateBackend provides abstract storage for timing wheel state.
type TickerStateBackend interface {
	// GetNextBucket returns the next bucket to process.
	// Returns -1 if no bucket has been processed yet (first run).
	GetNextBucket(ctx context.Context) (int64, error)

	// SaveNextBucket persists the next bucket number to process.
	SaveNextBucket(ctx context.Context, bucketNumber int64) error
}
