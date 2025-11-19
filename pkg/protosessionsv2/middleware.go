package newprotosessions

import (
	"context"

	"github.com/d8a-tech/d8a/pkg/hits"
)

// ==================== Identifier Clash Middleware ====================

// IdentifierClashMiddleware checks for identifier conflicts
type IdentifierClashMiddleware struct {
	identifierType    string
	extractIdentifier func(*hits.Hit) string
}

// NewIdentifierClashMiddleware creates a new identifier clash middleware
func NewIdentifierClashMiddleware(
	identifierType string,
	extractIdentifier func(*hits.Hit) string,
) BatchMiddleware {
	return &IdentifierClashMiddleware{
		identifierType:    identifierType,
		extractIdentifier: extractIdentifier,
	}
}

// Handle processes the batch and checks for identifier conflicts
func (m *IdentifierClashMiddleware) Handle(
	_ context.Context,
	batch *HitBatch,
	next func([]IOOperation) (IOResults, error),
) error {
	// Phase 1: Request conflict checks for all hits in batch
	checkOps := make([]IOOperation, 0, len(batch.Hits))
	for _, hit := range batch.Hits {
		checkOps = append(checkOps, &CheckIdentifierConflict{
			IdentifierType:   m.identifierType,
			IdentifierValue:  m.extractIdentifier(hit),
			ProposedClientID: hit.AuthoritativeClientID,
		})
	}

	// Execute conflict checks
	results, err := next(checkOps)
	if err != nil {
		return err
	}

	// Phase 2: Analyze results and prepare write operations
	writeOps := make([]IOOperation, 0)

	for _, result := range results.Filter(func(r IOResult) bool {
		_, ok := r.(*IdentifierConflictResult)
		return ok
	}) {
		conflictResult, ok := result.(*IdentifierConflictResult)
		if !ok {
			continue
		}

		if conflictResult.HasConflict {
			// Mark hit for eviction to existing client ID
			for _, hit := range batch.Hits {
				if m.extractIdentifier(hit) == conflictResult.Op.IdentifierValue {
					batch.MarkForEviction(hit, conflictResult.ExistingClientID)
				}
			}
		} else {
			// No conflict: store the mapping
			writeOps = append(writeOps, &MapIdentifierToClient{
				IdentifierType:  conflictResult.Op.IdentifierType,
				IdentifierValue: conflictResult.Op.IdentifierValue,
				ClientID:        conflictResult.Op.ProposedClientID,
			})
		}
	}

	// Execute write operations if any
	if len(writeOps) > 0 {
		_, err = next(writeOps)
		if err != nil {
			return err
		}
	}

	return nil
}
