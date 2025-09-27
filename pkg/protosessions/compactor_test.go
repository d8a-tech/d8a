package protosessions

import (
	"bytes"
	"testing"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testHit is a custom hit struct that wraps a Hit and overrides the Size method
type testHit struct {
	*hits.Hit
	sizeValue uint32
}

// Size overrides the original Size method
func (h *testHit) Size() uint32 {
	return h.sizeValue
}

func newTestHit(sizeValue uint32) *testHit {
	return &testHit{
		Hit:       hits.New(),
		sizeValue: sizeValue,
	}
}

func TestCompactorMiddleware_Handle(t *testing.T) {
	tests := []struct {
		name           string
		thresholdBytes uint32
		hitSize        uint32
		hitCount       int
		expectCompact  bool
	}{
		{
			name:           "should not compact when size is below threshold",
			thresholdBytes: 1000,
			hitSize:        100,
			hitCount:       1,
			expectCompact:  false,
		},
		{
			name:           "should compact when size exceeds threshold",
			thresholdBytes: 100,
			hitSize:        60,
			hitCount:       2,
			expectCompact:  true,
		},
		{
			name:           "should compact when size equals threshold",
			thresholdBytes: 100,
			hitSize:        100,
			hitCount:       1,
			expectCompact:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			kv := storage.NewInMemoryKV()
			set := storage.NewInMemorySet()

			cm := NewCompactorMiddleware(
				kv,
				encoding.JSONEncoder,
				encoding.JSONDecoder,
				tt.thresholdBytes,
			)
			compactor, ok := cm.(*compactorMiddleware)
			require.True(t, ok, "Failed to cast to compactorMiddleware")

			ctx := &Context{
				StorageKV:      kv,
				StorageSet:     set,
				Encoder:        encoding.JSONEncoder,
				Decoder:        encoding.JSONDecoder,
				allMiddlewares: []Middleware{compactor},
			}

			clientID := hits.ClientID("test-client-1")

			// Create and store hits
			for i := 0; i < tt.hitCount; i++ {
				// Create a test hit with custom size
				testHit := newTestHit(tt.hitSize)
				testHit.AuthoritativeClientID = clientID

				// Store the hit in the set (simulating collection endpoint)
				b := bytes.NewBuffer(nil)
				_, err := ctx.Encoder(b, testHit.Hit)
				require.NoError(t, err)
				err = ctx.StorageSet.Add([]byte(ProtoSessionHitsKey(testHit.AuthoritativeClientID)), b.Bytes())
				require.NoError(t, err)
			}

			// Last hit for triggering compact operation
			lastTestHit := newTestHit(tt.hitSize)
			lastTestHit.AuthoritativeClientID = clientID

			// when
			nextCalled := false
			err := compactor.Handle(ctx, lastTestHit.Hit, func() error {
				nextCalled = true
				return nil
			})

			// then
			require.NoError(t, err)
			assert.True(t, nextCalled, "Next handler should have been called")

			// Check if compaction occurred
			compactedBytes, err := kv.Get([]byte(CompactedHitsKey(clientID)))
			require.NoError(t, err)

			if tt.expectCompact {
				assert.NotNil(t, compactedBytes, "Expected hits to be compacted")

				// Verify all hits were collected
				var decodedHits []*hits.Hit
				err = encoding.JSONDecoder(bytes.NewReader(compactedBytes), &decodedHits)
				require.NoError(t, err)

				// Count should be the number of stored hits, not including the lastHit
				// since it's only used to trigger compaction but not stored yet
				assert.Equal(t, tt.hitCount, len(decodedHits), "Compacted hits count doesn't match")

				// Verify original hits were deleted
				originalHits, err := set.All([]byte(ProtoSessionHitsKey(clientID)))
				require.NoError(t, err)
				assert.Empty(t, originalHits, "Original hits should have been deleted after compaction")

				// Size map should be reset
				assert.Equal(t, uint32(0), compactor.size[clientID], "Size map should be reset after compaction")
			} else {
				assert.Nil(t, compactedBytes, "Hits should not be compacted")
			}
		})
	}
}

func TestCompactorMiddleware_Handle_EmptyHits(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	thresholdBytes := uint32(100)

	cm := NewCompactorMiddleware(
		kv,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
		thresholdBytes,
	)
	compactor, ok := cm.(*compactorMiddleware)
	require.True(t, ok, "Failed to cast to compactorMiddleware")

	ctx := &Context{
		StorageKV:      kv,
		StorageSet:     set,
		Encoder:        encoding.JSONEncoder,
		Decoder:        encoding.JSONDecoder,
		allMiddlewares: []Middleware{compactor},
	}

	clientID := hits.ClientID("test-client-empty")

	// Manually set the size over threshold without adding actual hits
	compactor.size[clientID] = thresholdBytes + 1

	// Create a test hit with custom size
	testHit := newTestHit(10) // Size doesn't matter for this test
	testHit.AuthoritativeClientID = clientID

	// when
	nextCalled := false
	err := compactor.Handle(ctx, testHit.Hit, func() error {
		nextCalled = true
		return nil
	})

	// then
	require.NoError(t, err)
	assert.True(t, nextCalled)

	// No hits to compact, so nothing should happen
	compactedBytes, err := kv.Get([]byte(CompactedHitsKey(clientID)))
	require.NoError(t, err)
	assert.Nil(t, compactedBytes, "No compaction should happen with empty hits")
}

func TestCompactorMiddleware_Handle_NextError(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	cm := NewCompactorMiddleware(
		kv,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
		100,
	)
	compactor, ok := cm.(*compactorMiddleware)
	require.True(t, ok, "Failed to cast to compactorMiddleware")

	ctx := &Context{
		StorageKV:      kv,
		StorageSet:     storage.NewInMemorySet(),
		Encoder:        encoding.JSONEncoder,
		Decoder:        encoding.JSONDecoder,
		allMiddlewares: []Middleware{compactor},
	}

	hit := hits.New()

	// when
	err := compactor.Handle(ctx, hit, func() error {
		return assert.AnError
	})

	// then
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

func TestCompactorMiddleware_OnCleanup(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	cm := NewCompactorMiddleware(
		kv,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
		100,
	)
	compactor, ok := cm.(*compactorMiddleware)
	require.True(t, ok, "Failed to cast to compactorMiddleware")

	ctx := &Context{
		StorageKV:      kv,
		StorageSet:     storage.NewInMemorySet(),
		Encoder:        encoding.JSONEncoder,
		Decoder:        encoding.JSONDecoder,
		allMiddlewares: []Middleware{compactor},
	}

	clientID := hits.ClientID("test-client-cleanup")

	// Add some data to the size map
	compactor.size[clientID] = 500

	// when
	err := compactor.OnCleanup(ctx, clientID)

	// then
	require.NoError(t, err)
	_, exists := compactor.size[clientID]
	assert.False(t, exists, "Size entry should be deleted after cleanup")
}

func TestCompactorMiddleware_OnCollect(t *testing.T) {
	tests := []struct {
		name          string
		compactedData []*hits.Hit
		expectError   bool
	}{
		{
			name:          "should return empty slice when no compacted hits exist",
			compactedData: nil,
			expectError:   false,
		},
		{
			name:          "should return stored hits when compacted hits exist",
			compactedData: []*hits.Hit{hits.New(), hits.New()},
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			kv := storage.NewInMemoryKV()
			cm := NewCompactorMiddleware(
				kv,
				encoding.JSONEncoder,
				encoding.JSONDecoder,
				100,
			)
			compactor, ok := cm.(*compactorMiddleware)
			require.True(t, ok, "Failed to cast to compactorMiddleware")

			ctx := &Context{
				StorageKV:      kv,
				StorageSet:     storage.NewInMemorySet(),
				Encoder:        encoding.JSONEncoder,
				Decoder:        encoding.JSONDecoder,
				allMiddlewares: []Middleware{compactor},
			}

			clientID := hits.ClientID("test-client-collect")

			// Store compacted hits if needed
			if tt.compactedData != nil {
				b := bytes.NewBuffer(nil)
				_, err := encoding.JSONEncoder(b, tt.compactedData)
				require.NoError(t, err)
				_, err = kv.Set([]byte(CompactedHitsKey(clientID)), b.Bytes())
				require.NoError(t, err)
			}

			// when
			result, err := compactor.OnCollect(ctx, clientID)

			// then
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.compactedData == nil {
					assert.Empty(t, result, "Should return empty slice when no compacted hits exist")
				} else {
					assert.Equal(t, len(tt.compactedData), len(result), "Should return all compacted hits")
				}
			}
		})
	}
}

func TestCompactorMiddleware_addSize(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	cm := NewCompactorMiddleware(
		kv,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
		100,
	)
	compactor, ok := cm.(*compactorMiddleware)
	require.True(t, ok, "Failed to cast to compactorMiddleware")

	clientID := hits.ClientID("test-client-add-size")

	// when: first add
	size1 := compactor.addSize(clientID, 50)

	// then
	assert.Equal(t, uint32(50), size1, "First addSize should return 50")
	assert.Equal(t, uint32(50), compactor.size[clientID], "Size should be 50 after first add")

	// when: second add
	size2 := compactor.addSize(clientID, 60)

	// then
	assert.Equal(t, uint32(110), size2, "Second addSize should return 110")
	assert.Equal(t, uint32(110), compactor.size[clientID], "Size should be 110 after second add")
}
