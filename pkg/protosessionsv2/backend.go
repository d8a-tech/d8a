// Package newprotosessions provides a batching architecture for protosession processing.
package newprotosessions

import (
	"context"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
)

// ==================== Generic KV/Set Backend ====================

// GenericStorageBackend implements StorageBackend using storage.KV and storage.Set
type GenericStorageBackend struct {
	kv      storage.KV
	set     storage.Set
	encoder encoding.EncoderFunc
	decoder encoding.DecoderFunc
}

// NewGenericStorageBackend creates a backend using generic KV and Set abstractions
func NewGenericStorageBackend(
	kv storage.KV,
	set storage.Set,
	encoder encoding.EncoderFunc,
	decoder encoding.DecoderFunc,
) StorageBackend {
	return &GenericStorageBackend{
		kv:      kv,
		set:     set,
		encoder: encoder,
		decoder: decoder,
	}
}

// ExecuteBatch executes a batch of operations using storage.KV and storage.Set
func (b *GenericStorageBackend) ExecuteBatch(_ context.Context, operations []IOOperation) (IOResults, error) {
	results := make([]IOResult, 0, len(operations))

	for _, op := range operations {
		switch o := op.(type) {
		case *CheckIdentifierConflict:
			key := []byte(identifierKey(o.IdentifierType, o.IdentifierValue))
			val, err := b.kv.Get(key)

			hasConflict := false
			existingID := hits.ClientID("")
			if val != nil {
				existingID = hits.ClientID(val)
				hasConflict = existingID != o.ProposedClientID
			}

			results = append(results, &IdentifierConflictResult{
				Op:               o,
				HasConflict:      hasConflict,
				ExistingClientID: existingID,
				Err:              err,
			})

		case *MapIdentifierToClient:
			key := []byte(identifierKey(o.IdentifierType, o.IdentifierValue))
			_, err := b.kv.Set(key, []byte(o.ClientID))
			results = append(results, &GenericResult{Op: o, Err: err})

		default:
			results = append(results, &GenericResult{
				Op:  o,
				Err: fmt.Errorf("unsupported operation: %T", o),
			})
		}
	}

	return newIOResults(results), nil
}

func identifierKey(identifierType, identifierValue string) string {
	return fmt.Sprintf("identifier.%s.%s", identifierType, identifierValue)
}
