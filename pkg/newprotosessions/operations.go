package newprotosessions

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/hits"
)

// ==================== Business Operations ====================

// CheckIdentifierConflict checks if an identifier maps to a different client
type CheckIdentifierConflict struct {
	IdentifierType   string
	IdentifierValue  string
	ProposedClientID hits.ClientID
}

// Describe returns a human-readable description of the operation
func (o *CheckIdentifierConflict) Describe() string {
	return fmt.Sprintf("CheckIdentifierConflict(%s:%s->%s)",
		o.IdentifierType, o.IdentifierValue, o.ProposedClientID)
}

// IdentifierConflictResult is the result of checking identifier conflicts
type IdentifierConflictResult struct {
	Op               *CheckIdentifierConflict
	HasConflict      bool
	ExistingClientID hits.ClientID
	Err              error
}

// Operation returns the operation that produced this result
func (r *IdentifierConflictResult) Operation() IOOperation { return r.Op }

// Error returns the error if any occurred during operation
func (r *IdentifierConflictResult) Error() error { return r.Err }

// MapIdentifierToClient stores identifier → clientID mapping
type MapIdentifierToClient struct {
	IdentifierType  string
	IdentifierValue string
	ClientID        hits.ClientID
}

// Describe returns a human-readable description of the operation
func (o *MapIdentifierToClient) Describe() string {
	return fmt.Sprintf("MapIdentifierToClient(%s:%s->%s)",
		o.IdentifierType, o.IdentifierValue, o.ClientID)
}

// GenericResult is a generic operation result
type GenericResult struct {
	Op  IOOperation
	Err error
}

// Operation returns the operation that produced this result
func (r *GenericResult) Operation() IOOperation { return r.Op }

// Error returns the error if any occurred during operation
func (r *GenericResult) Error() error { return r.Err }
