package protosessionsv3

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

func TestNaiveGenericStorageBatchedIOBackend_HandleBatch_AppendHits(t *testing.T) {
	tests := []struct {
		name         string
		requests     []*AppendHitsToProtoSessionRequest
		expectErrors []bool
	}{
		{
			name: "should append hits to single proto-session",
			requests: []*AppendHitsToProtoSessionRequest{
				{
					ProtoSessionID: "client1",
					Hits: []*hits.Hit{
						{AuthoritativeClientID: "client1", IP: "192.168.1.1"},
						{AuthoritativeClientID: "client1", IP: "192.168.1.2"},
					},
				},
			},
			expectErrors: []bool{false},
		},
		{
			name: "should append hits to multiple proto-sessions",
			requests: []*AppendHitsToProtoSessionRequest{
				{
					ProtoSessionID: "client1",
					Hits:           []*hits.Hit{{AuthoritativeClientID: "client1"}},
				},
				{
					ProtoSessionID: "client2",
					Hits:           []*hits.Hit{{AuthoritativeClientID: "client2"}},
				},
			},
			expectErrors: []bool{false, false},
		},
		{
			name: "should handle empty hits list",
			requests: []*AppendHitsToProtoSessionRequest{
				{
					ProtoSessionID: "client1",
					Hits:           []*hits.Hit{},
				},
			},
			expectErrors: []bool{false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			kv := storage.NewInMemoryKV()
			set := storage.NewInMemorySet()
			backend := NewNaiveGenericStorageBatchedIOBackend(
				kv,
				set,
				encoding.JSONEncoder,
				encoding.JSONDecoder,
			)

			// when
			appendResponses, _, _ := backend.HandleBatch(
				context.Background(),
				tt.requests,
				nil,
				nil,
			)

			// then
			require.Len(t, appendResponses, len(tt.requests))
			for i, expectError := range tt.expectErrors {
				if expectError {
					assert.Error(t, appendResponses[i].Err)
				} else {
					assert.NoError(t, appendResponses[i].Err)
				}
			}

			// Verify hits were stored
			for _, req := range tt.requests {
				if len(req.Hits) > 0 {
					storedHits, err := set.All([]byte(protoSessionHitsKey(req.ProtoSessionID)))
					require.NoError(t, err)
					assert.Len(t, storedHits, len(req.Hits))
				}
			}
		})
	}
}

func TestNaiveGenericStorageBatchedIOBackend_HandleBatch_GetHits(t *testing.T) {
	tests := []struct {
		name         string
		setupHits    map[hits.ClientID][]*hits.Hit
		requests     []*GetProtoSessionHitsRequest
		expectCounts []int
	}{
		{
			name: "should get hits for single proto-session",
			setupHits: map[hits.ClientID][]*hits.Hit{
				"client1": {
					{AuthoritativeClientID: "client1", IP: "192.168.1.1"},
					{AuthoritativeClientID: "client1", IP: "192.168.1.2"},
				},
			},
			requests: []*GetProtoSessionHitsRequest{
				{ProtoSessionID: "client1"},
			},
			expectCounts: []int{2},
		},
		{
			name: "should get hits for multiple proto-sessions",
			setupHits: map[hits.ClientID][]*hits.Hit{
				"client1": {{AuthoritativeClientID: "client1", IP: "10.0.0.1"}},
				"client2": {
					{AuthoritativeClientID: "client2", IP: "10.0.0.2"},
					{AuthoritativeClientID: "client2", IP: "10.0.0.3"},
				},
			},
			requests: []*GetProtoSessionHitsRequest{
				{ProtoSessionID: "client1"},
				{ProtoSessionID: "client2"},
			},
			expectCounts: []int{1, 2},
		},
		{
			name:      "should return empty for non-existent proto-session",
			setupHits: map[hits.ClientID][]*hits.Hit{},
			requests: []*GetProtoSessionHitsRequest{
				{ProtoSessionID: "non-existent"},
			},
			expectCounts: []int{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			kv := storage.NewInMemoryKV()
			set := storage.NewInMemorySet()
			backend := NewNaiveGenericStorageBatchedIOBackend(
				kv,
				set,
				encoding.JSONEncoder,
				encoding.JSONDecoder,
			)

			// Setup: store hits using append
			for clientID, hitsToStore := range tt.setupHits {
				_, _, _ = backend.HandleBatch(
					context.Background(),
					[]*AppendHitsToProtoSessionRequest{
						{ProtoSessionID: clientID, Hits: hitsToStore},
					},
					nil,
					nil,
				)
			}

			// when
			_, getResponses, _ := backend.HandleBatch(
				context.Background(),
				nil,
				tt.requests,
				nil,
			)

			// then
			require.Len(t, getResponses, len(tt.requests))
			for i, expectCount := range tt.expectCounts {
				assert.NoError(t, getResponses[i].Err)
				assert.Len(t, getResponses[i].Hits, expectCount)
			}
		})
	}
}

func TestNaiveGenericStorageBatchedIOBackend_HandleBatch_MarkClosing(t *testing.T) {
	tests := []struct {
		name     string
		requests []*MarkProtoSessionClosingForGivenBucketRequest
	}{
		{
			name: "should mark single proto-session for bucket",
			requests: []*MarkProtoSessionClosingForGivenBucketRequest{
				{ProtoSessionID: "client1", BucketID: 100},
			},
		},
		{
			name: "should mark multiple proto-sessions for different buckets",
			requests: []*MarkProtoSessionClosingForGivenBucketRequest{
				{ProtoSessionID: "client1", BucketID: 100},
				{ProtoSessionID: "client2", BucketID: 101},
				{ProtoSessionID: "client3", BucketID: 100},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			kv := storage.NewInMemoryKV()
			set := storage.NewInMemorySet()
			backend := NewNaiveGenericStorageBatchedIOBackend(
				kv,
				set,
				encoding.JSONEncoder,
				encoding.JSONDecoder,
			)

			// when
			_, _, markResponses := backend.HandleBatch(
				context.Background(),
				nil,
				nil,
				tt.requests,
			)

			// then
			require.Len(t, markResponses, len(tt.requests))
			for i := range tt.requests {
				assert.NoError(t, markResponses[i].Err)
			}

			// Verify proto-sessions were added to buckets
			buckets := make(map[int64]int)
			for _, req := range tt.requests {
				buckets[req.BucketID]++
			}

			for bucketID, expectedCount := range buckets {
				members, err := set.All([]byte(bucketsKey(bucketID)))
				require.NoError(t, err)
				assert.Len(t, members, expectedCount)
			}
		})
	}
}

func TestNaiveGenericStorageBatchedIOBackend_HandleBatch_Combined(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewNaiveGenericStorageBatchedIOBackend(
		kv,
		set,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
	)

	appendRequests := []*AppendHitsToProtoSessionRequest{
		{
			ProtoSessionID: "client1",
			Hits:           []*hits.Hit{{AuthoritativeClientID: "client1"}},
		},
	}
	getRequests := []*GetProtoSessionHitsRequest{
		{ProtoSessionID: "client1"},
	}
	markRequests := []*MarkProtoSessionClosingForGivenBucketRequest{
		{ProtoSessionID: "client1", BucketID: 100},
	}

	// when
	appendResponses, getResponses, markResponses := backend.HandleBatch(
		context.Background(),
		appendRequests,
		getRequests,
		markRequests,
	)

	// then
	require.Len(t, appendResponses, 1)
	assert.NoError(t, appendResponses[0].Err)

	require.Len(t, getResponses, 1)
	assert.NoError(t, getResponses[0].Err)
	assert.Len(t, getResponses[0].Hits, 1)

	require.Len(t, markResponses, 1)
	assert.NoError(t, markResponses[0].Err)
}

func TestNaiveGenericStorageBatchedIOBackend_GetAllProtosessionsForBucket(t *testing.T) {
	tests := []struct {
		name                string
		setupProtoSessions  map[hits.ClientID][]*hits.Hit
		bucketID            int64
		expectedSessionsLen int
	}{
		{
			name: "should get all proto-sessions for bucket with single session",
			setupProtoSessions: map[hits.ClientID][]*hits.Hit{
				"client1": {
					{AuthoritativeClientID: "client1", IP: "192.168.1.1"},
					{AuthoritativeClientID: "client1", IP: "192.168.1.2"},
				},
			},
			bucketID:            100,
			expectedSessionsLen: 1,
		},
		{
			name: "should get all proto-sessions for bucket with multiple sessions",
			setupProtoSessions: map[hits.ClientID][]*hits.Hit{
				"client1": {{AuthoritativeClientID: "client1"}},
				"client2": {{AuthoritativeClientID: "client2"}},
				"client3": {{AuthoritativeClientID: "client3"}},
			},
			bucketID:            100,
			expectedSessionsLen: 3,
		},
		{
			name:                "should return empty for bucket with no proto-sessions",
			setupProtoSessions:  map[hits.ClientID][]*hits.Hit{},
			bucketID:            100,
			expectedSessionsLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			kv := storage.NewInMemoryKV()
			set := storage.NewInMemorySet()
			backend := NewNaiveGenericStorageBatchedIOBackend(
				kv,
				set,
				encoding.JSONEncoder,
				encoding.JSONDecoder,
			)

			// Setup: append hits and mark for bucket
			for clientID, hitsToStore := range tt.setupProtoSessions {
				backend.HandleBatch(
					context.Background(),
					[]*AppendHitsToProtoSessionRequest{
						{ProtoSessionID: clientID, Hits: hitsToStore},
					},
					nil,
					[]*MarkProtoSessionClosingForGivenBucketRequest{
						{ProtoSessionID: clientID, BucketID: tt.bucketID},
					},
				)
			}

			// when
			responses := backend.GetAllProtosessionsForBucket(
				context.Background(),
				[]*GetAllProtosessionsForBucketRequest{
					{BucketID: tt.bucketID},
				},
			)

			// then
			require.Len(t, responses, 1)
			assert.NoError(t, responses[0].Err)
			assert.Len(t, responses[0].ProtoSessions, tt.expectedSessionsLen)

			// Verify each proto-session has correct hits
			for i, session := range responses[0].ProtoSessions {
				assert.NotEmpty(t, session, "Session %d should not be empty", i)
			}
		})
	}
}

func TestNaiveGenericStorageBatchedIOBackend_GetAllProtosessionsForBucket_Multiple(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewNaiveGenericStorageBatchedIOBackend(
		kv,
		set,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
	)

	// Setup proto-sessions in different buckets
	backend.HandleBatch(
		context.Background(),
		[]*AppendHitsToProtoSessionRequest{
			{ProtoSessionID: "client1", Hits: []*hits.Hit{{AuthoritativeClientID: "client1"}}},
			{ProtoSessionID: "client2", Hits: []*hits.Hit{{AuthoritativeClientID: "client2"}}},
		},
		nil,
		[]*MarkProtoSessionClosingForGivenBucketRequest{
			{ProtoSessionID: "client1", BucketID: 100},
			{ProtoSessionID: "client2", BucketID: 200},
		},
	)

	// when
	responses := backend.GetAllProtosessionsForBucket(
		context.Background(),
		[]*GetAllProtosessionsForBucketRequest{
			{BucketID: 100},
			{BucketID: 200},
			{BucketID: 300},
		},
	)

	// then
	require.Len(t, responses, 3)
	assert.NoError(t, responses[0].Err)
	assert.Len(t, responses[0].ProtoSessions, 1)
	assert.NoError(t, responses[1].Err)
	assert.Len(t, responses[1].ProtoSessions, 1)
	assert.NoError(t, responses[2].Err)
	assert.Len(t, responses[2].ProtoSessions, 0)
}

func TestNaiveGenericStorageBatchedIOBackend_RemoveProtoSessionHits(t *testing.T) {
	tests := []struct {
		name              string
		setupHits         map[hits.ClientID][]*hits.Hit
		removeRequests    []*RemoveProtoSessionHitsRequest
		verifyClientID    hits.ClientID
		expectHitsRemoved bool
	}{
		{
			name: "should remove hits for single proto-session",
			setupHits: map[hits.ClientID][]*hits.Hit{
				"client1": {{AuthoritativeClientID: "client1"}},
			},
			removeRequests: []*RemoveProtoSessionHitsRequest{
				{ProtoSessionID: "client1"},
			},
			verifyClientID:    "client1",
			expectHitsRemoved: true,
		},
		{
			name: "should remove hits for multiple proto-sessions",
			setupHits: map[hits.ClientID][]*hits.Hit{
				"client1": {{AuthoritativeClientID: "client1"}},
				"client2": {{AuthoritativeClientID: "client2"}},
			},
			removeRequests: []*RemoveProtoSessionHitsRequest{
				{ProtoSessionID: "client1"},
				{ProtoSessionID: "client2"},
			},
			verifyClientID:    "client1",
			expectHitsRemoved: true,
		},
		{
			name:      "should handle removing non-existent proto-session",
			setupHits: map[hits.ClientID][]*hits.Hit{},
			removeRequests: []*RemoveProtoSessionHitsRequest{
				{ProtoSessionID: "non-existent"},
			},
			verifyClientID:    "non-existent",
			expectHitsRemoved: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			kv := storage.NewInMemoryKV()
			set := storage.NewInMemorySet()
			backend := NewNaiveGenericStorageBatchedIOBackend(
				kv,
				set,
				encoding.JSONEncoder,
				encoding.JSONDecoder,
			)

			// Setup: store hits
			for clientID, hitsToStore := range tt.setupHits {
				backend.HandleBatch(
					context.Background(),
					[]*AppendHitsToProtoSessionRequest{
						{ProtoSessionID: clientID, Hits: hitsToStore},
					},
					nil,
					nil,
				)
			}

			// when
			responses := backend.RemoveProtoSessionHits(
				context.Background(),
				tt.removeRequests,
			)

			// then
			require.Len(t, responses, len(tt.removeRequests))
			for i := range tt.removeRequests {
				assert.NoError(t, responses[i].Err)
			}

			// Verify hits were removed
			if tt.expectHitsRemoved {
				storedHits, err := set.All([]byte(protoSessionHitsKey(tt.verifyClientID)))
				require.NoError(t, err)
				assert.Empty(t, storedHits)
			}
		})
	}
}

func TestNaiveGenericStorageBatchedIOBackend_CleanupMachinery(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewNaiveGenericStorageBatchedIOBackend(
		kv,
		set,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
	)

	// when
	err := backend.CleanupMachinery(context.Background())

	// then
	assert.NoError(t, err)
}

func TestNaiveGenericStorageBatchedIOBackend_Integration(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewNaiveGenericStorageBatchedIOBackend(
		kv,
		set,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
	)

	client1 := hits.ClientID("client1")
	client2 := hits.ClientID("client2")
	bucketID := int64(100)

	hit1 := &hits.Hit{
		AuthoritativeClientID: client1,
		IP:                    "192.168.1.1",
		ServerReceivedTime:    time.Now(),
	}
	hit2 := &hits.Hit{
		AuthoritativeClientID: client2,
		IP:                    "192.168.1.2",
		ServerReceivedTime:    time.Now(),
	}

	// when: append hits
	appendResponses, _, _ := backend.HandleBatch(
		context.Background(),
		[]*AppendHitsToProtoSessionRequest{
			{ProtoSessionID: client1, Hits: []*hits.Hit{hit1}},
			{ProtoSessionID: client2, Hits: []*hits.Hit{hit2}},
		},
		nil,
		nil,
	)

	// then
	require.Len(t, appendResponses, 2)
	assert.NoError(t, appendResponses[0].Err)
	assert.NoError(t, appendResponses[1].Err)

	// when: mark for closing
	_, _, markResponses := backend.HandleBatch(
		context.Background(),
		nil,
		nil,
		[]*MarkProtoSessionClosingForGivenBucketRequest{
			{ProtoSessionID: client1, BucketID: bucketID},
			{ProtoSessionID: client2, BucketID: bucketID},
		},
	)

	// then
	require.Len(t, markResponses, 2)
	assert.NoError(t, markResponses[0].Err)
	assert.NoError(t, markResponses[1].Err)

	// when: get all for bucket
	bucketResponses := backend.GetAllProtosessionsForBucket(
		context.Background(),
		[]*GetAllProtosessionsForBucketRequest{
			{BucketID: bucketID},
		},
	)

	// then
	require.Len(t, bucketResponses, 1)
	assert.NoError(t, bucketResponses[0].Err)
	assert.Len(t, bucketResponses[0].ProtoSessions, 2)

	// when: remove hits
	removeResponses := backend.RemoveProtoSessionHits(
		context.Background(),
		[]*RemoveProtoSessionHitsRequest{
			{ProtoSessionID: client1},
			{ProtoSessionID: client2},
		},
	)

	// then
	require.Len(t, removeResponses, 2)
	assert.NoError(t, removeResponses[0].Err)
	assert.NoError(t, removeResponses[1].Err)

	// when: verify removal
	_, getResponses, _ := backend.HandleBatch(
		context.Background(),
		nil,
		[]*GetProtoSessionHitsRequest{
			{ProtoSessionID: client1},
			{ProtoSessionID: client2},
		},
		nil,
	)

	// then
	require.Len(t, getResponses, 2)
	assert.NoError(t, getResponses[0].Err)
	assert.Empty(t, getResponses[0].Hits)
	assert.NoError(t, getResponses[1].Err)
	assert.Empty(t, getResponses[1].Hits)

	// when: cleanup
	err := backend.CleanupMachinery(context.Background())

	// then
	assert.NoError(t, err)
}

func TestNaiveGenericStorageBatchedIOBackend_GetIdentifierConflicts(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewNaiveGenericStorageBatchedIOBackend(kv, set, encoding.JSONEncoder, encoding.JSONDecoder)

	// when
	results := backend.GetIdentifierConflicts(context.Background(), []*IdentifierConflictRequest{
		{
			IdentifierType: "session_stamp",
			Hit: &hits.Hit{
				AuthoritativeClientID: "client1",
				IP:                    "192.168.1.1",
			},
			ExtractIdentifier: func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		},
		{
			IdentifierType: "session_stamp",
			Hit: &hits.Hit{
				AuthoritativeClientID: "client1",
				IP:                    "192.168.1.1",
			},
			ExtractIdentifier: func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		},
		{
			IdentifierType: "session_stamp",
			Hit: &hits.Hit{
				AuthoritativeClientID: "client2",
				IP:                    "192.168.1.1",
			},
			ExtractIdentifier: func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		},
	})

	// then
	require.Len(t, results, 3)
	require.NoError(t, results[0].Err)
	require.False(t, results[0].HasConflict)
	require.Equal(t, hits.ClientID(""), results[0].ConflictsWith)
	require.NoError(t, results[1].Err)
	require.False(t, results[1].HasConflict)
	require.Equal(t, hits.ClientID(""), results[1].ConflictsWith)
	require.NoError(t, results[2].Err)
	require.True(t, results[2].HasConflict)
	require.Equal(t, hits.ClientID("client1"), results[2].ConflictsWith)
}
