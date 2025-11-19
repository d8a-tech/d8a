package newprotosessions

import (
	"context"

	"github.com/d8a-tech/d8a/pkg/hits"
)

// ==================== Core Interfaces ====================

// IOOperation represents a business-level operation
type IOOperation interface {
	Describe() string
}

// IOResult represents the result of an operation
type IOResult interface {
	Operation() IOOperation
	Error() error
}

// IOResults provides access to operation results
type IOResults interface {
	Get(op IOOperation) (IOResult, bool)
	Filter(fn func(IOResult) bool) []IOResult
	All() []IOResult
}

// StorageBackend implements business operations optimally
type StorageBackend interface {
	ExecuteBatch(ctx context.Context, operations []IOOperation) (IOResults, error)
}

// BatchMiddleware processes a batch of hits
type BatchMiddleware interface {
	Handle(ctx context.Context, batch *HitBatch, next func([]IOOperation) (IOResults, error)) error
}

// ==================== HitBatch ====================

// HitBatch holds a batch of hits with metadata
type HitBatch struct {
	Hits             []*hits.Hit
	evictionMetadata map[hits.ClientID]hits.ClientID // current -> target
}

// MarkForEviction marks a hit for eviction to another client ID
func (b *HitBatch) MarkForEviction(hit *hits.Hit, targetClientID hits.ClientID) {
	if b.evictionMetadata == nil {
		b.evictionMetadata = make(map[hits.ClientID]hits.ClientID)
	}
	b.evictionMetadata[hit.AuthoritativeClientID] = targetClientID
}

// IsMarkedForEviction checks if a hit is marked for eviction
func (b *HitBatch) IsMarkedForEviction(hit *hits.Hit) bool {
	if b.evictionMetadata == nil {
		return false
	}
	_, exists := b.evictionMetadata[hit.AuthoritativeClientID]
	return exists
}

// UniqueClientIDs returns unique client IDs in the batch
func (b *HitBatch) UniqueClientIDs() []hits.ClientID {
	seen := make(map[hits.ClientID]bool)
	result := make([]hits.ClientID, 0)
	for _, hit := range b.Hits {
		if !seen[hit.AuthoritativeClientID] {
			seen[hit.AuthoritativeClientID] = true
			result = append(result, hit.AuthoritativeClientID)
		}
	}
	return result
}

// ==================== IOResults Implementation ====================

type ioResults struct {
	results []IOResult
}

func newIOResults(results []IOResult) IOResults {
	return &ioResults{results: results}
}

func (r *ioResults) Get(op IOOperation) (IOResult, bool) {
	for _, result := range r.results {
		if result.Operation() == op {
			return result, true
		}
	}
	return nil, false
}

func (r *ioResults) Filter(fn func(IOResult) bool) []IOResult {
	filtered := make([]IOResult, 0)
	for _, result := range r.results {
		if fn(result) {
			filtered = append(filtered, result)
		}
	}
	return filtered
}

func (r *ioResults) All() []IOResult {
	return r.results
}
