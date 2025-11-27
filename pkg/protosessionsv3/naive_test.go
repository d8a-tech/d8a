package protosessionsv3

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/storage"
)

func TestNaiveGenericStorageBatchedIOBackend(t *testing.T) {
	factory := func() BatchedIOBackend {
		return NewNaiveGenericStorageBatchedIOBackend(
			storage.NewInMemoryKV(),
			storage.NewInMemorySet(),
			encoding.JSONEncoder,
			encoding.JSONDecoder,
		)
	}

	BatchedIOBackendTestSuite(t, factory)
}
