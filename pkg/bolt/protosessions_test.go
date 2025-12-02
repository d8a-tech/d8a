package bolt

import (
	"os"
	"testing"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/protosessionsv3"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestBoltBatchedIOBackend(t *testing.T) {
	factory := func() protosessionsv3.BatchedIOBackend {
		// Create temp file for each test
		f, err := os.CreateTemp("", "bolt-test-*.db")
		require.NoError(t, err)
		dbPath := f.Name()
		require.NoError(t, f.Close())

		db, err := bolt.Open(dbPath, 0o600, nil)
		require.NoError(t, err)

		backend, err := NewBatchedProtosessionsIOBackend(
			db,
			encoding.JSONEncoder,
			encoding.JSONDecoder,
		)
		require.NoError(t, err)

		// Register cleanup
		t.Cleanup(func() {
			_ = os.Remove(dbPath)
		})

		return backend
	}

	protosessionsv3.BatchedIOBackendTestSuite(t, factory)
}
