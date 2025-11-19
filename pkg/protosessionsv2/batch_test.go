package newprotosessions

import (
	"context"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentifierClashMiddleware_NoConflict(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewGenericStorageBackend(kv, set, encoding.JSONEncoder, encoding.JSONDecoder)

	middleware := NewIdentifierClashMiddleware(
		"session_stamp",
		func(h *hits.Hit) string { return h.SessionStamp() },
	)

	hit1 := hits.New()
	hit1.IP = "192.168.1.1"
	hit1.ServerReceivedTime = time.Now()

	hit2 := hits.New()
	hit2.IP = "192.168.1.2"
	hit2.ServerReceivedTime = time.Now()

	batch := &HitBatch{
		Hits: []*hits.Hit{hit1, hit2},
	}

	// when
	handler := Chain([]BatchMiddleware{middleware}, backend)
	err := handler(context.Background(), batch)

	// then
	require.NoError(t, err)
	assert.False(t, batch.IsMarkedForEviction(hit1))
	assert.False(t, batch.IsMarkedForEviction(hit2))

	// verify mappings were stored
	kvStore := kv.(*storage.InMemoryKV)
	assert.Contains(t, kvStore.KV, "identifier.session_stamp.192.168.1.1")
	assert.Contains(t, kvStore.KV, "identifier.session_stamp.192.168.1.2")
	assert.Equal(t, []byte(hit1.AuthoritativeClientID), kvStore.KV["identifier.session_stamp.192.168.1.1"])
	assert.Equal(t, []byte(hit2.AuthoritativeClientID), kvStore.KV["identifier.session_stamp.192.168.1.2"])
}

func TestIdentifierClashMiddleware_WithConflict(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewGenericStorageBackend(kv, set, encoding.JSONEncoder, encoding.JSONDecoder)

	middleware := NewIdentifierClashMiddleware(
		"session_stamp",
		func(h *hits.Hit) string { return h.SessionStamp() },
	)

	// First hit establishes the session stamp mapping
	firstHit := hits.New()
	firstHit.IP = "192.168.1.1"
	firstHit.ServerReceivedTime = time.Now()

	firstBatch := &HitBatch{Hits: []*hits.Hit{firstHit}}
	handler := Chain([]BatchMiddleware{middleware}, backend)
	err := handler(context.Background(), firstBatch)
	require.NoError(t, err)

	// Second hit with same session stamp but different client ID
	secondHit := hits.New()
	secondHit.IP = "192.168.1.1" // Same IP (session stamp)
	secondHit.ClientID = hits.ClientID("different-client-id")
	secondHit.AuthoritativeClientID = hits.ClientID("different-client-id")
	secondHit.ServerReceivedTime = time.Now()

	// when
	secondBatch := &HitBatch{Hits: []*hits.Hit{secondHit}}
	err = handler(context.Background(), secondBatch)

	// then
	require.NoError(t, err)
	assert.True(t, secondBatch.IsMarkedForEviction(secondHit))

	// verify mapping was not overwritten
	kvStore := kv.(*storage.InMemoryKV)
	assert.Equal(t, []byte(firstHit.AuthoritativeClientID), kvStore.KV["identifier.session_stamp.192.168.1.1"])
}

func TestIdentifierClashMiddleware_BatchWithMultipleHits(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewGenericStorageBackend(kv, set, encoding.JSONEncoder, encoding.JSONDecoder)

	middleware := NewIdentifierClashMiddleware(
		"session_stamp",
		func(h *hits.Hit) string { return h.SessionStamp() },
	)

	// Create batch with multiple hits with same session stamp
	hit1 := hits.New()
	hit1.IP = "192.168.1.1"
	hit1.ServerReceivedTime = time.Now()

	hit2 := hits.New()
	hit2.IP = "192.168.1.1" // Same session stamp
	hit2.ClientID = hit1.ClientID
	hit2.AuthoritativeClientID = hit1.AuthoritativeClientID
	hit2.ServerReceivedTime = time.Now()

	hit3 := hits.New()
	hit3.IP = "192.168.1.2" // Different session stamp
	hit3.ServerReceivedTime = time.Now()

	batch := &HitBatch{
		Hits: []*hits.Hit{hit1, hit2, hit3},
	}

	// when
	handler := Chain([]BatchMiddleware{middleware}, backend)
	err := handler(context.Background(), batch)

	// then
	require.NoError(t, err)
	assert.False(t, batch.IsMarkedForEviction(hit1))
	assert.False(t, batch.IsMarkedForEviction(hit2))
	assert.False(t, batch.IsMarkedForEviction(hit3))

	// verify mappings
	kvStore := kv.(*storage.InMemoryKV)
	assert.Len(t, kvStore.KV, 2) // Two unique session stamps
}

func TestGenericStorageBackend_CheckIdentifierConflict(t *testing.T) {
	tests := []struct {
		name             string
		existingMapping  map[string]string // identifier -> clientID
		checkIdentifier  string
		checkClientID    hits.ClientID
		expectConflict   bool
		expectExistingID hits.ClientID
	}{
		{
			name:             "no existing mapping",
			existingMapping:  map[string]string{},
			checkIdentifier:  "stamp1",
			checkClientID:    "client1",
			expectConflict:   false,
			expectExistingID: "",
		},
		{
			name:             "existing mapping matches",
			existingMapping:  map[string]string{"stamp1": "client1"},
			checkIdentifier:  "stamp1",
			checkClientID:    "client1",
			expectConflict:   false,
			expectExistingID: "client1",
		},
		{
			name:             "existing mapping conflicts",
			existingMapping:  map[string]string{"stamp1": "client1"},
			checkIdentifier:  "stamp1",
			checkClientID:    "client2",
			expectConflict:   true,
			expectExistingID: "client1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			kv := storage.NewInMemoryKV()
			for identifier, clientID := range tt.existingMapping {
				_, err := kv.Set([]byte("identifier.session_stamp."+identifier), []byte(clientID))
				require.NoError(t, err)
			}

			backend := NewGenericStorageBackend(
				kv,
				storage.NewInMemorySet(),
				encoding.JSONEncoder,
				encoding.JSONDecoder,
			)

			// when
			results, err := backend.ExecuteBatch(context.Background(), []IOOperation{
				&CheckIdentifierConflict{
					IdentifierType:   "session_stamp",
					IdentifierValue:  tt.checkIdentifier,
					ProposedClientID: tt.checkClientID,
				},
			})

			// then
			require.NoError(t, err)
			allResults := results.All()
			require.Len(t, allResults, 1)

			conflictResult, ok := allResults[0].(*IdentifierConflictResult)
			require.True(t, ok)
			assert.Equal(t, tt.expectConflict, conflictResult.HasConflict)
			assert.Equal(t, tt.expectExistingID, conflictResult.ExistingClientID)
		})
	}
}

func TestGenericStorageBackend_MapIdentifierToClient(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	backend := NewGenericStorageBackend(
		kv,
		storage.NewInMemorySet(),
		encoding.JSONEncoder,
		encoding.JSONDecoder,
	)

	// when
	results, err := backend.ExecuteBatch(context.Background(), []IOOperation{
		&MapIdentifierToClient{
			IdentifierType:  "session_stamp",
			IdentifierValue: "stamp1",
			ClientID:        "client1",
		},
		&MapIdentifierToClient{
			IdentifierType:  "session_stamp",
			IdentifierValue: "stamp2",
			ClientID:        "client2",
		},
	})

	// then
	require.NoError(t, err)
	assert.Len(t, results.All(), 2)

	kvStore := kv.(*storage.InMemoryKV)
	assert.Equal(t, []byte("client1"), kvStore.KV["identifier.session_stamp.stamp1"])
	assert.Equal(t, []byte("client2"), kvStore.KV["identifier.session_stamp.stamp2"])
}

func TestChain_MultipleMiddlewares(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewGenericStorageBackend(kv, set, encoding.JSONEncoder, encoding.JSONDecoder)

	executionOrder := []string{}

	middleware1 := &testMiddleware{
		handleFunc: func(ctx context.Context, batch *HitBatch, next func([]IOOperation) (IOResults, error)) error {
			executionOrder = append(executionOrder, "middleware1-before")
			_, err := next([]IOOperation{})
			if err != nil {
				return err
			}
			executionOrder = append(executionOrder, "middleware1-after")
			return nil
		},
	}

	middleware2 := &testMiddleware{
		handleFunc: func(ctx context.Context, batch *HitBatch, next func([]IOOperation) (IOResults, error)) error {
			executionOrder = append(executionOrder, "middleware2-before")
			_, err := next([]IOOperation{})
			if err != nil {
				return err
			}
			executionOrder = append(executionOrder, "middleware2-after")
			return nil
		},
	}

	batch := &HitBatch{Hits: []*hits.Hit{}}

	// when
	handler := Chain([]BatchMiddleware{middleware1, middleware2}, backend)
	err := handler(context.Background(), batch)

	// then
	require.NoError(t, err)
	assert.Equal(t, []string{
		"middleware1-before",
		"middleware2-before",
		"middleware2-after",
		"middleware1-after",
	}, executionOrder)
}

func TestNewBatchHandler(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewGenericStorageBackend(kv, set, encoding.JSONEncoder, encoding.JSONDecoder)

	middleware := NewIdentifierClashMiddleware(
		"session_stamp",
		func(h *hits.Hit) string { return h.SessionStamp() },
	)

	handler := NewBatchHandler(
		context.Background(),
		backend,
		[]BatchMiddleware{middleware},
	)

	hit := hits.New()
	hit.IP = "192.168.1.1"
	hit.ServerReceivedTime = time.Now()

	// when
	err := handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{hit},
	})

	// then
	assert.Nil(t, err)

	kvStore := kv.(*storage.InMemoryKV)
	assert.Contains(t, kvStore.KV, "identifier.session_stamp.192.168.1.1")
}

func TestHitBatch_UniqueClientIDs(t *testing.T) {
	// given
	hit1 := hits.New()
	hit2 := hits.New()
	hit3 := hits.New()
	hit3.AuthoritativeClientID = hit1.AuthoritativeClientID // Duplicate

	batch := &HitBatch{
		Hits: []*hits.Hit{hit1, hit2, hit3},
	}

	// when
	unique := batch.UniqueClientIDs()

	// then
	assert.Len(t, unique, 2)
}

func TestIOResults_Filter(t *testing.T) {
	// given
	results := newIOResults([]IOResult{
		&IdentifierConflictResult{HasConflict: true},
		&IdentifierConflictResult{HasConflict: false},
		&GenericResult{},
	})

	// when
	conflictResults := results.Filter(func(r IOResult) bool {
		_, ok := r.(*IdentifierConflictResult)
		return ok
	})

	// then
	assert.Len(t, conflictResults, 2)
}

// Test helpers

type testMiddleware struct {
	handleFunc func(context.Context, *HitBatch, func([]IOOperation) (IOResults, error)) error
}

func (m *testMiddleware) Handle(
	ctx context.Context,
	batch *HitBatch,
	next func([]IOOperation) (IOResults, error),
) error {
	return m.handleFunc(ctx, batch, next)
}
