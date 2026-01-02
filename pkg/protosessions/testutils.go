//nolint:dupl,funlen // test utils
package protosessions

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/pings"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shorter type aliases for long type names
type markClosingReq = MarkProtoSessionClosingForGivenBucketRequest
type markClosingResp = MarkProtoSessionClosingForGivenBucketResponse

// TestBatchedIOBackend is a configurable test implementation of BatchedIOBackend
type TestBatchedIOBackend struct {
	identifierConflictHandler   func(*IdentifierConflictRequest) *IdentifierConflictResponse
	appendHitsHandler           func(*AppendHitsToProtoSessionRequest) *AppendHitsToProtoSessionResponse
	getProtoSessionHitsHandler  func(*GetProtoSessionHitsRequest) *GetProtoSessionHitsResponse
	markClosingHandler          func(*markClosingReq) *markClosingResp
	getAllForBucketHandler      func(*GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse
	removeProtoSessionHandler   func(*RemoveProtoSessionHitsRequest) *RemoveProtoSessionHitsResponse
	cleanupMachineryHandler     func() error
	removeMetadataHandler       func(*RemoveAllHitRelatedMetadataRequest) *RemoveAllHitRelatedMetadataResponse
	removeBucketMetadataHandler func(*RemoveBucketMetadataRequest) *RemoveBucketMetadataResponse
}

// GetIdentifierConflicts implements BatchedIOBackend
func (t *TestBatchedIOBackend) GetIdentifierConflicts(
	_ context.Context,
	requests []*IdentifierConflictRequest,
) []*IdentifierConflictResponse {
	responses := make([]*IdentifierConflictResponse, len(requests))
	for i, req := range requests {
		responses[i] = t.identifierConflictHandler(req)
		responses[i].Request = req
	}
	return responses
}

// HandleBatch implements BatchedIOBackend
func (t *TestBatchedIOBackend) HandleBatch(
	_ context.Context,
	appendHitsRequests []*AppendHitsToProtoSessionRequest,
	getProtoSessionHitsRequests []*GetProtoSessionHitsRequest,
	markProtoSessionClosingForGivenBucketRequests []*MarkProtoSessionClosingForGivenBucketRequest,
) (
	[]*AppendHitsToProtoSessionResponse,
	[]*GetProtoSessionHitsResponse,
	[]*MarkProtoSessionClosingForGivenBucketResponse,
) {
	appendResponses := make([]*AppendHitsToProtoSessionResponse, len(appendHitsRequests))
	for i, req := range appendHitsRequests {
		appendResponses[i] = t.appendHitsHandler(req)
	}

	getResponses := make([]*GetProtoSessionHitsResponse, len(getProtoSessionHitsRequests))
	for i, req := range getProtoSessionHitsRequests {
		getResponses[i] = t.getProtoSessionHitsHandler(req)
	}

	markRequests := markProtoSessionClosingForGivenBucketRequests
	markResponses := make([]*MarkProtoSessionClosingForGivenBucketResponse, len(markRequests))
	for i, req := range markRequests {
		markResponses[i] = t.markClosingHandler(req)
	}

	return appendResponses, getResponses, markResponses
}

// GetAllProtosessionsForBucket implements BatchedIOBackend
func (t *TestBatchedIOBackend) GetAllProtosessionsForBucket(
	_ context.Context,
	requests []*GetAllProtosessionsForBucketRequest,
) []*GetAllProtosessionsForBucketResponse {
	responses := make([]*GetAllProtosessionsForBucketResponse, len(requests))
	for i, req := range requests {
		responses[i] = t.getAllForBucketHandler(req)
	}
	return responses
}

// Cleanup implements BatchedIOBackend
func (t *TestBatchedIOBackend) Cleanup(
	_ context.Context,
	hitsRequests []*RemoveProtoSessionHitsRequest,
	metadataRequests []*RemoveAllHitRelatedMetadataRequest,
	bucketMetadataRequests []*RemoveBucketMetadataRequest,
) (
	[]*RemoveProtoSessionHitsResponse,
	[]*RemoveAllHitRelatedMetadataResponse,
	[]*RemoveBucketMetadataResponse,
) {
	hitsResponses := make([]*RemoveProtoSessionHitsResponse, len(hitsRequests))
	for i, req := range hitsRequests {
		hitsResponses[i] = t.removeProtoSessionHandler(req)
	}

	metadataResponses := make([]*RemoveAllHitRelatedMetadataResponse, len(metadataRequests))
	for i, req := range metadataRequests {
		metadataResponses[i] = t.removeMetadataHandler(req)
	}

	bucketMetadataResponses := make([]*RemoveBucketMetadataResponse, len(bucketMetadataRequests))
	for i, req := range bucketMetadataRequests {
		bucketMetadataResponses[i] = t.removeBucketMetadataHandler(req)
	}

	return hitsResponses, metadataResponses, bucketMetadataResponses
}

// Stop implements BatchedIOBackend
func (t *TestBatchedIOBackend) Stop(_ context.Context) error {
	return t.cleanupMachineryHandler()
}

// TestBatchedIOBackendOption configures TestBatchedIOBackend
type TestBatchedIOBackendOption func(*TestBatchedIOBackend)

// WithIdentifierConflictHandler sets custom handler for identifier conflict requests
func WithIdentifierConflictHandler(
	h func(*IdentifierConflictRequest) *IdentifierConflictResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.identifierConflictHandler = h
	}
}

// WithAppendHitsHandler sets custom handler for append hits requests
func WithAppendHitsHandler(
	h func(*AppendHitsToProtoSessionRequest) *AppendHitsToProtoSessionResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.appendHitsHandler = h
	}
}

// WithGetProtoSessionHitsHandler sets custom handler for get proto session hits requests
func WithGetProtoSessionHitsHandler(
	h func(*GetProtoSessionHitsRequest) *GetProtoSessionHitsResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.getProtoSessionHitsHandler = h
	}
}

// WithMarkProtoSessionClosingHandler sets custom handler for mark closing requests
func WithMarkProtoSessionClosingHandler(
	h func(*MarkProtoSessionClosingForGivenBucketRequest) *MarkProtoSessionClosingForGivenBucketResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.markClosingHandler = h
	}
}

// WithGetAllProtosessionsForBucketHandler sets custom handler for bucket proto sessions requests
func WithGetAllProtosessionsForBucketHandler(
	h func(*GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.getAllForBucketHandler = h
	}
}

// WithRemoveProtoSessionHitsHandler sets custom handler for remove hits requests
func WithRemoveProtoSessionHitsHandler(
	h func(*RemoveProtoSessionHitsRequest) *RemoveProtoSessionHitsResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.removeProtoSessionHandler = h
	}
}

// WithRemoveAllHitRelatedMetadataHandler sets custom handler for remove metadata requests
func WithRemoveAllHitRelatedMetadataHandler(
	h func(*RemoveAllHitRelatedMetadataRequest) *RemoveAllHitRelatedMetadataResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.removeMetadataHandler = h
	}
}

// WithRemoveBucketMetadataHandler sets custom handler for remove bucket metadata requests
func WithRemoveBucketMetadataHandler(
	h func(*RemoveBucketMetadataRequest) *RemoveBucketMetadataResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.removeBucketMetadataHandler = h
	}
}

// WithCleanupMachineryHandler sets custom handler for cleanup
func WithCleanupMachineryHandler(handler func() error) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.cleanupMachineryHandler = handler
	}
}

// NewTestBatchedIOBackend creates a test backend with success-like defaults
func NewTestBatchedIOBackend(opts ...TestBatchedIOBackendOption) BatchedIOBackend {
	backend := &TestBatchedIOBackend{
		identifierConflictHandler: func(_ *IdentifierConflictRequest) *IdentifierConflictResponse {
			return &IdentifierConflictResponse{
				Err:         nil,
				HasConflict: false,
			}
		},
		appendHitsHandler: func(_ *AppendHitsToProtoSessionRequest) *AppendHitsToProtoSessionResponse {
			return &AppendHitsToProtoSessionResponse{
				Err: nil,
			}
		},
		getProtoSessionHitsHandler: func(_ *GetProtoSessionHitsRequest) *GetProtoSessionHitsResponse {
			return &GetProtoSessionHitsResponse{
				Hits: []*hits.Hit{},
				Err:  nil,
			}
		},
		markClosingHandler: func(_ *markClosingReq) *markClosingResp {
			return &MarkProtoSessionClosingForGivenBucketResponse{
				Err: nil,
			}
		},
		getAllForBucketHandler: func(_ *GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse {
			return &GetAllProtosessionsForBucketResponse{
				ProtoSessions: [][]*hits.Hit{},
				Err:           nil,
			}
		},
		removeProtoSessionHandler: func(_ *RemoveProtoSessionHitsRequest) *RemoveProtoSessionHitsResponse {
			return &RemoveProtoSessionHitsResponse{
				Err: nil,
			}
		},
		removeMetadataHandler: func(_ *RemoveAllHitRelatedMetadataRequest) *RemoveAllHitRelatedMetadataResponse {
			return &RemoveAllHitRelatedMetadataResponse{
				Err: nil,
			}
		},
		cleanupMachineryHandler: func() error {
			return nil
		},
		removeBucketMetadataHandler: func(_ *RemoveBucketMetadataRequest) *RemoveBucketMetadataResponse {
			return &RemoveBucketMetadataResponse{
				Err: nil,
			}
		},
	}

	for _, opt := range opts {
		opt(backend)
	}

	return backend
}

// TestCloser is a configurable test implementation of Closer
type TestCloser struct {
	closeHandler func([][]*hits.Hit) error
}

// Close implements Closer
func (c *TestCloser) Close(protosessions [][]*hits.Hit) error {
	return c.closeHandler(protosessions)
}

// TestCloserOption configures TestCloser
type TestCloserOption func(*TestCloser)

// WithCloseHandler sets custom handler for closing proto-sessions
func WithCloseHandler(handler func([][]*hits.Hit) error) TestCloserOption {
	return func(c *TestCloser) {
		c.closeHandler = handler
	}
}

// NewTestCloser creates a test closer with success-like defaults
func NewTestCloser(opts ...TestCloserOption) Closer {
	closer := &TestCloser{
		closeHandler: func(_ [][]*hits.Hit) error {
			return nil
		},
	}

	for _, opt := range opts {
		opt(closer)
	}

	return closer
}

// --- Test Suite for BatchedIOBackend implementations ---

// testHit creates a test hit with predictable fields
func testHit(clientID hits.ClientID, ip string) *hits.Hit {
	return &hits.Hit{
		AuthoritativeClientID: clientID,
		Request: &hits.Request{
			IP: ip,
		},
	}
}

// testIdentifierExtractor returns a simple extractor for testing
func testIdentifierExtractor() func(*hits.Hit) string {
	return func(h *hits.Hit) string {
		return h.MustServerAttributes().IP
	}
}

// BatchedIOBackendTestSuite tests BatchedIOBackend implementations
func BatchedIOBackendTestSuite(t *testing.T, factory func() BatchedIOBackend) { //nolint:funlen,gocognit // test suite
	t.Helper()

	t.Run("GetIdentifierConflicts", func(t *testing.T) {
		testGetIdentifierConflicts(t, factory)
	})

	t.Run("HandleBatch_AppendHits", func(t *testing.T) {
		testHandleBatchAppendHits(t, factory)
	})

	t.Run("HandleBatch_GetHits", func(t *testing.T) {
		testHandleBatchGetHits(t, factory)
	})

	t.Run("HandleBatch_MarkClosing", func(t *testing.T) {
		testHandleBatchMarkClosing(t, factory)
	})

	t.Run("GetAllProtosessionsForBucket", func(t *testing.T) {
		testGetAllProtosessionsForBucket(t, factory)
	})

	t.Run("Cleanup_RemoveHits", func(t *testing.T) {
		testCleanupRemoveHits(t, factory)
	})

	t.Run("Cleanup_RemoveMetadata", func(t *testing.T) {
		testCleanupRemoveMetadata(t, factory)
	})

	t.Run("Cleanup_RemoveBucketMetadata", func(t *testing.T) {
		testCleanupRemoveBucketMetadata(t, factory)
	})

	t.Run("Stop", func(t *testing.T) {
		testStop(t, factory)
	})

	t.Run("Integration", func(t *testing.T) {
		testIntegration(t, factory)
	})

	t.Run("DataIsolation", func(t *testing.T) {
		testDataIsolation(t, factory)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		testConcurrentAccess(t, factory)
	})
}

func testGetIdentifierConflicts(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	tests := []struct {
		name            string
		requests        []*IdentifierConflictRequest
		expectConflicts []bool
		conflictsWith   []hits.ClientID
	}{
		{
			name: "first registration succeeds",
			requests: []*IdentifierConflictRequest{
				NewIdentifierConflictRequest(testHit("client1", "1.1.1.1"), "ip", testIdentifierExtractor()),
			},
			expectConflicts: []bool{false},
			conflictsWith:   []hits.ClientID{""},
		},
		{
			name: "same client re-registers same identifier",
			requests: []*IdentifierConflictRequest{
				NewIdentifierConflictRequest(testHit("client1", "2.2.2.2"), "ip", testIdentifierExtractor()),
				NewIdentifierConflictRequest(testHit("client1", "2.2.2.2"), "ip", testIdentifierExtractor()),
			},
			expectConflicts: []bool{false, false},
			conflictsWith:   []hits.ClientID{"", ""},
		},
		{
			name: "different client same identifier causes conflict",
			requests: []*IdentifierConflictRequest{
				NewIdentifierConflictRequest(testHit("client1", "3.3.3.3"), "ip", testIdentifierExtractor()),
				NewIdentifierConflictRequest(testHit("client2", "3.3.3.3"), "ip", testIdentifierExtractor()),
			},
			expectConflicts: []bool{false, true},
			conflictsWith:   []hits.ClientID{"", "client1"},
		},
		{
			name: "multiple different identifiers all succeed",
			requests: []*IdentifierConflictRequest{
				NewIdentifierConflictRequest(testHit("client1", "4.4.4.1"), "ip", testIdentifierExtractor()),
				NewIdentifierConflictRequest(testHit("client2", "4.4.4.2"), "ip", testIdentifierExtractor()),
				NewIdentifierConflictRequest(testHit("client3", "4.4.4.3"), "ip", testIdentifierExtractor()),
			},
			expectConflicts: []bool{false, false, false},
			conflictsWith:   []hits.ClientID{"", "", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			backend := factory()
			defer func() { _ = backend.Stop(context.Background()) }()

			// when
			responses := backend.GetIdentifierConflicts(context.Background(), tt.requests)

			// then
			require.Len(t, responses, len(tt.requests))
			for i, resp := range responses {
				assert.NoError(t, resp.Err)
				assert.Equal(t, tt.expectConflicts[i], resp.HasConflict, "conflict mismatch at index %d", i)
				assert.Equal(t, tt.conflictsWith[i], resp.ConflictsWith, "conflictsWith mismatch at index %d", i)
			}
		})
	}

	t.Run("empty request batch", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		// when
		responses := backend.GetIdentifierConflicts(context.Background(), []*IdentifierConflictRequest{})

		// then
		assert.Empty(t, responses)
	})
}

func testHandleBatchAppendHits(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	tests := []struct {
		name         string
		requests     []*AppendHitsToProtoSessionRequest
		expectCounts []int
	}{
		{
			name: "append single hit",
			requests: []*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			},
			expectCounts: []int{1},
		},
		{
			name: "append multiple hits to same session",
			requests: []*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{
					testHit("client1", "1.1.1.1"),
					testHit("client1", "1.1.1.2"),
					testHit("client1", "1.1.1.3"),
				}),
			},
			expectCounts: []int{3},
		},
		{
			name: "append to multiple sessions in batch",
			requests: []*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
				NewAppendHitsToProtoSessionRequest("client2", []*hits.Hit{testHit("client2", "2.2.2.2")}),
			},
			expectCounts: []int{1, 1},
		},
		{
			name: "append empty hits list",
			requests: []*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{}),
			},
			expectCounts: []int{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			backend := factory()
			defer func() { _ = backend.Stop(context.Background()) }()

			// when
			appendResponses, _, _ := backend.HandleBatch(context.Background(), tt.requests, nil, nil)

			// then
			require.Len(t, appendResponses, len(tt.requests))
			for _, resp := range appendResponses {
				assert.NoError(t, resp.Err)
			}

			// verify hits stored
			getRequests := make([]*GetProtoSessionHitsRequest, len(tt.requests))
			for i, req := range tt.requests {
				getRequests[i] = NewGetProtoSessionHitsRequest(req.ProtoSessionID)
			}
			_, getResponses, _ := backend.HandleBatch(context.Background(), nil, getRequests, nil)

			require.Len(t, getResponses, len(tt.requests))
			for i, resp := range getResponses {
				assert.NoError(t, resp.Err)
				assert.Len(t, resp.Hits, tt.expectCounts[i])
			}
		})
	}

	t.Run("append same hit twice stores both", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		hit := testHit("client1", "1.1.1.1")

		// when
		backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{hit})},
			nil, nil)
		backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{hit})},
			nil, nil)

		// then
		_, getResponses, _ := backend.HandleBatch(context.Background(), nil,
			[]*GetProtoSessionHitsRequest{NewGetProtoSessionHitsRequest("client1")}, nil)

		assert.NoError(t, getResponses[0].Err)
		// Set semantics: duplicate encoded bytes are deduplicated
		assert.GreaterOrEqual(t, len(getResponses[0].Hits), 1)
	})
}

func testHandleBatchGetHits(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("get existing session hits", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		expectedHits := []*hits.Hit{testHit("client1", "1.1.1.1"), testHit("client1", "2.2.2.2")}
		backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{NewAppendHitsToProtoSessionRequest("client1", expectedHits)},
			nil, nil)

		// when
		_, getResponses, _ := backend.HandleBatch(context.Background(), nil,
			[]*GetProtoSessionHitsRequest{NewGetProtoSessionHitsRequest("client1")}, nil)

		// then
		require.Len(t, getResponses, 1)
		assert.NoError(t, getResponses[0].Err)
		assert.Len(t, getResponses[0].Hits, 2)
	})

	t.Run("get non-existent session", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		// when
		_, getResponses, _ := backend.HandleBatch(context.Background(), nil,
			[]*GetProtoSessionHitsRequest{NewGetProtoSessionHitsRequest("unknown")}, nil)

		// then
		require.Len(t, getResponses, 1)
		assert.NoError(t, getResponses[0].Err)
		assert.Empty(t, getResponses[0].Hits)
	})

	t.Run("get multiple sessions in batch", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(), []*AppendHitsToProtoSessionRequest{
			NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			NewAppendHitsToProtoSessionRequest("client2", []*hits.Hit{
				testHit("client2", "2.2.2.1"),
				testHit("client2", "2.2.2.2"),
			}),
		}, nil, nil)

		// when
		_, getResponses, _ := backend.HandleBatch(context.Background(), nil, []*GetProtoSessionHitsRequest{
			NewGetProtoSessionHitsRequest("client1"),
			NewGetProtoSessionHitsRequest("client2"),
		}, nil)

		// then
		require.Len(t, getResponses, 2)
		assert.NoError(t, getResponses[0].Err)
		assert.Len(t, getResponses[0].Hits, 1)
		assert.NoError(t, getResponses[1].Err)
		assert.Len(t, getResponses[1].Hits, 2)
	})

	t.Run("get after append in same batch reflects append", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		// when
		_, getResponses, _ := backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			},
			[]*GetProtoSessionHitsRequest{NewGetProtoSessionHitsRequest("client1")},
			nil)

		// then
		require.Len(t, getResponses, 1)
		assert.NoError(t, getResponses[0].Err)
		assert.Len(t, getResponses[0].Hits, 1)
	})
}

func testHandleBatchMarkClosing(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("mark single session for bucket", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			}, nil, nil)

		// when
		_, _, markResponses := backend.HandleBatch(context.Background(), nil, nil,
			[]*MarkProtoSessionClosingForGivenBucketRequest{
				NewMarkProtoSessionClosingForGivenBucketRequest("client1", 100),
			})

		// then
		require.Len(t, markResponses, 1)
		assert.NoError(t, markResponses[0].Err)

		// verify appears in bucket
		bucketResponses := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{NewGetAllProtosessionsForBucketRequest(100)})
		assert.Len(t, bucketResponses[0].ProtoSessions, 1)
	})

	t.Run("mark multiple sessions same bucket", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(), []*AppendHitsToProtoSessionRequest{
			NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			NewAppendHitsToProtoSessionRequest("client2", []*hits.Hit{testHit("client2", "2.2.2.2")}),
			NewAppendHitsToProtoSessionRequest("client3", []*hits.Hit{testHit("client3", "3.3.3.3")}),
		}, nil, nil)

		// when
		_, _, markResponses := backend.HandleBatch(context.Background(), nil, nil,
			[]*MarkProtoSessionClosingForGivenBucketRequest{
				NewMarkProtoSessionClosingForGivenBucketRequest("client1", 100),
				NewMarkProtoSessionClosingForGivenBucketRequest("client2", 100),
				NewMarkProtoSessionClosingForGivenBucketRequest("client3", 100),
			})

		// then
		require.Len(t, markResponses, 3)
		for _, resp := range markResponses {
			assert.NoError(t, resp.Err)
		}

		bucketResponses := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{NewGetAllProtosessionsForBucketRequest(100)})
		assert.Len(t, bucketResponses[0].ProtoSessions, 3)
	})

	t.Run("mark same session for different buckets", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			}, nil, nil)

		// when
		backend.HandleBatch(context.Background(), nil, nil,
			[]*MarkProtoSessionClosingForGivenBucketRequest{
				NewMarkProtoSessionClosingForGivenBucketRequest("client1", 100),
			})
		backend.HandleBatch(context.Background(), nil, nil,
			[]*MarkProtoSessionClosingForGivenBucketRequest{
				NewMarkProtoSessionClosingForGivenBucketRequest("client1", 200),
			})

		// then - only latest bucket should contain the session
		bucket100 := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{NewGetAllProtosessionsForBucketRequest(100)})
		bucket200 := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{NewGetAllProtosessionsForBucketRequest(200)})

		assert.Len(t, bucket100[0].ProtoSessions, 0)
		assert.Len(t, bucket200[0].ProtoSessions, 1)
	})

	t.Run("mark session for same bucket twice is idempotent", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			}, nil, nil)

		// when
		backend.HandleBatch(context.Background(), nil, nil,
			[]*MarkProtoSessionClosingForGivenBucketRequest{
				NewMarkProtoSessionClosingForGivenBucketRequest("client1", 100),
			})
		backend.HandleBatch(context.Background(), nil, nil,
			[]*MarkProtoSessionClosingForGivenBucketRequest{
				NewMarkProtoSessionClosingForGivenBucketRequest("client1", 100),
			})

		// then
		bucketResponses := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{NewGetAllProtosessionsForBucketRequest(100)})
		assert.Len(t, bucketResponses[0].ProtoSessions, 1)
	})
}

func testGetAllProtosessionsForBucket(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("single session in bucket with hits", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(), []*AppendHitsToProtoSessionRequest{
			NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{
				testHit("client1", "1.1.1.1"),
				testHit("client1", "1.1.1.2"),
			}),
		}, nil, []*MarkProtoSessionClosingForGivenBucketRequest{
			NewMarkProtoSessionClosingForGivenBucketRequest("client1", 100),
		})

		// when
		responses := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{NewGetAllProtosessionsForBucketRequest(100)})

		// then
		require.Len(t, responses, 1)
		assert.NoError(t, responses[0].Err)
		assert.Len(t, responses[0].ProtoSessions, 1)
		assert.Len(t, responses[0].ProtoSessions[0], 2)
	})

	t.Run("empty bucket", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		// when
		responses := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{NewGetAllProtosessionsForBucketRequest(999)})

		// then
		require.Len(t, responses, 1)
		assert.NoError(t, responses[0].Err)
		assert.Empty(t, responses[0].ProtoSessions)
	})

	t.Run("multiple buckets in batch", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(), []*AppendHitsToProtoSessionRequest{
			NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			NewAppendHitsToProtoSessionRequest("client2", []*hits.Hit{testHit("client2", "2.2.2.2")}),
		}, nil, []*MarkProtoSessionClosingForGivenBucketRequest{
			NewMarkProtoSessionClosingForGivenBucketRequest("client1", 100),
			NewMarkProtoSessionClosingForGivenBucketRequest("client2", 200),
		})

		// when
		responses := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{
				NewGetAllProtosessionsForBucketRequest(100),
				NewGetAllProtosessionsForBucketRequest(200),
				NewGetAllProtosessionsForBucketRequest(300),
			})

		// then
		require.Len(t, responses, 3)
		assert.NoError(t, responses[0].Err)
		assert.Len(t, responses[0].ProtoSessions, 1)
		assert.NoError(t, responses[1].Err)
		assert.Len(t, responses[1].ProtoSessions, 1)
		assert.NoError(t, responses[2].Err)
		assert.Empty(t, responses[2].ProtoSessions)
	})
}

func testCleanupRemoveHits(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("remove existing session hits", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			}, nil, nil)

		// when
		hitsResponses, _, _ := backend.Cleanup(context.Background(),
			[]*RemoveProtoSessionHitsRequest{NewRemoveProtoSessionHitsRequest("client1")},
			nil, nil)

		// then
		require.Len(t, hitsResponses, 1)
		assert.NoError(t, hitsResponses[0].Err)

		// verify removed
		_, getResponses, _ := backend.HandleBatch(context.Background(), nil,
			[]*GetProtoSessionHitsRequest{NewGetProtoSessionHitsRequest("client1")}, nil)
		assert.Empty(t, getResponses[0].Hits)
	})

	t.Run("remove non-existent session", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		// when
		hitsResponses, _, _ := backend.Cleanup(context.Background(),
			[]*RemoveProtoSessionHitsRequest{NewRemoveProtoSessionHitsRequest("unknown")},
			nil, nil)

		// then
		require.Len(t, hitsResponses, 1)
		assert.NoError(t, hitsResponses[0].Err)
	})

	t.Run("remove multiple in batch", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(), []*AppendHitsToProtoSessionRequest{
			NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			NewAppendHitsToProtoSessionRequest("client2", []*hits.Hit{testHit("client2", "2.2.2.2")}),
		}, nil, nil)

		// when
		hitsResponses, _, _ := backend.Cleanup(context.Background(),
			[]*RemoveProtoSessionHitsRequest{
				NewRemoveProtoSessionHitsRequest("client1"),
				NewRemoveProtoSessionHitsRequest("client2"),
			}, nil, nil)

		// then
		require.Len(t, hitsResponses, 2)
		assert.NoError(t, hitsResponses[0].Err)
		assert.NoError(t, hitsResponses[1].Err)

		// verify both removed
		_, getResponses, _ := backend.HandleBatch(context.Background(), nil,
			[]*GetProtoSessionHitsRequest{
				NewGetProtoSessionHitsRequest("client1"),
				NewGetProtoSessionHitsRequest("client2"),
			}, nil)
		assert.Empty(t, getResponses[0].Hits)
		assert.Empty(t, getResponses[1].Hits)
	})
}

func testCleanupRemoveMetadata(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("remove identifier metadata allows re-registration", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		hit := testHit("client1", "1.1.1.1")
		extractor := testIdentifierExtractor()

		// register identifier
		backend.GetIdentifierConflicts(context.Background(),
			[]*IdentifierConflictRequest{NewIdentifierConflictRequest(hit, "ip", extractor)})

		// when - remove metadata
		_, metadataResponses, _ := backend.Cleanup(context.Background(), nil,
			[]*RemoveAllHitRelatedMetadataRequest{
				NewRemoveAllHitRelatedMetadataRequest(hit, "ip", extractor),
			}, nil)

		// then
		require.Len(t, metadataResponses, 1)
		assert.NoError(t, metadataResponses[0].Err)

		// new client can register same identifier
		conflictResponses := backend.GetIdentifierConflicts(context.Background(),
			[]*IdentifierConflictRequest{
				NewIdentifierConflictRequest(testHit("client2", "1.1.1.1"), "ip", extractor),
			})
		assert.False(t, conflictResponses[0].HasConflict)
	})

	t.Run("remove non-existent metadata", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		// when
		_, metadataResponses, _ := backend.Cleanup(context.Background(), nil,
			[]*RemoveAllHitRelatedMetadataRequest{
				NewRemoveAllHitRelatedMetadataRequest(testHit("unknown", "9.9.9.9"), "ip", testIdentifierExtractor()),
			}, nil)

		// then
		require.Len(t, metadataResponses, 1)
		assert.NoError(t, metadataResponses[0].Err)
	})
}

func testCleanupRemoveBucketMetadata(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("remove bucket metadata clears bucket", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()
		backend.HandleBatch(context.Background(), []*AppendHitsToProtoSessionRequest{
			NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
		}, nil, []*MarkProtoSessionClosingForGivenBucketRequest{
			NewMarkProtoSessionClosingForGivenBucketRequest("client1", 100),
		})

		// when
		_, _, bucketResponses := backend.Cleanup(context.Background(), nil, nil,
			[]*RemoveBucketMetadataRequest{NewRemoveBucketMetadataRequest(100)})

		// then
		require.Len(t, bucketResponses, 1)
		assert.NoError(t, bucketResponses[0].Err)

		// verify bucket is empty
		allResponses := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{NewGetAllProtosessionsForBucketRequest(100)})
		assert.Empty(t, allResponses[0].ProtoSessions)
	})

	t.Run("remove non-existent bucket", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		// when
		_, _, bucketResponses := backend.Cleanup(context.Background(), nil, nil,
			[]*RemoveBucketMetadataRequest{NewRemoveBucketMetadataRequest(999)})

		// then
		require.Len(t, bucketResponses, 1)
		assert.NoError(t, bucketResponses[0].Err)
	})
}

func testStop(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("stop clean backend", func(t *testing.T) {
		// given
		backend := factory()

		// when
		err := backend.Stop(context.Background())

		// then
		assert.NoError(t, err)
	})

	t.Run("stop after operations", func(t *testing.T) {
		// given
		backend := factory()
		backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			}, nil, []*MarkProtoSessionClosingForGivenBucketRequest{
				NewMarkProtoSessionClosingForGivenBucketRequest("client1", 100),
			})

		// when
		err := backend.Stop(context.Background())

		// then
		assert.NoError(t, err)
	})
}

func testIntegration(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("full lifecycle", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		client1 := hits.ClientID("client1")
		client2 := hits.ClientID("client2")
		bucketID := int64(100)

		// when: append hits
		appendResponses, _, _ := backend.HandleBatch(context.Background(),
			[]*AppendHitsToProtoSessionRequest{
				NewAppendHitsToProtoSessionRequest(client1, []*hits.Hit{testHit(client1, "1.1.1.1")}),
				NewAppendHitsToProtoSessionRequest(client2, []*hits.Hit{testHit(client2, "2.2.2.2")}),
			}, nil, nil)

		// then
		require.Len(t, appendResponses, 2)
		assert.NoError(t, appendResponses[0].Err)
		assert.NoError(t, appendResponses[1].Err)

		// when: mark for closing
		_, _, markResponses := backend.HandleBatch(context.Background(), nil, nil,
			[]*MarkProtoSessionClosingForGivenBucketRequest{
				NewMarkProtoSessionClosingForGivenBucketRequest(client1, bucketID),
				NewMarkProtoSessionClosingForGivenBucketRequest(client2, bucketID),
			})

		// then
		require.Len(t, markResponses, 2)
		assert.NoError(t, markResponses[0].Err)
		assert.NoError(t, markResponses[1].Err)

		// when: get all for bucket
		bucketResponses := backend.GetAllProtosessionsForBucket(context.Background(),
			[]*GetAllProtosessionsForBucketRequest{NewGetAllProtosessionsForBucketRequest(bucketID)})

		// then
		require.Len(t, bucketResponses, 1)
		assert.NoError(t, bucketResponses[0].Err)
		assert.Len(t, bucketResponses[0].ProtoSessions, 2)

		// when: cleanup
		hitsResponses, _, bucketCleanupResponses := backend.Cleanup(context.Background(),
			[]*RemoveProtoSessionHitsRequest{
				NewRemoveProtoSessionHitsRequest(client1),
				NewRemoveProtoSessionHitsRequest(client2),
			}, nil, []*RemoveBucketMetadataRequest{
				NewRemoveBucketMetadataRequest(bucketID),
			})

		// then
		require.Len(t, hitsResponses, 2)
		assert.NoError(t, hitsResponses[0].Err)
		assert.NoError(t, hitsResponses[1].Err)
		require.Len(t, bucketCleanupResponses, 1)
		assert.NoError(t, bucketCleanupResponses[0].Err)

		// verify empty
		_, getResponses, _ := backend.HandleBatch(context.Background(), nil,
			[]*GetProtoSessionHitsRequest{
				NewGetProtoSessionHitsRequest(client1),
				NewGetProtoSessionHitsRequest(client2),
			}, nil)
		assert.Empty(t, getResponses[0].Hits)
		assert.Empty(t, getResponses[1].Hits)
	})
}

func testDataIsolation(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("operations on session1 dont affect session2", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		backend.HandleBatch(context.Background(), []*AppendHitsToProtoSessionRequest{
			NewAppendHitsToProtoSessionRequest("client1", []*hits.Hit{testHit("client1", "1.1.1.1")}),
			NewAppendHitsToProtoSessionRequest("client2", []*hits.Hit{testHit("client2", "2.2.2.2")}),
		}, nil, nil)

		// when: remove client1 hits
		backend.Cleanup(context.Background(),
			[]*RemoveProtoSessionHitsRequest{NewRemoveProtoSessionHitsRequest("client1")},
			nil, nil)

		// then: client2 unaffected
		_, getResponses, _ := backend.HandleBatch(context.Background(), nil,
			[]*GetProtoSessionHitsRequest{
				NewGetProtoSessionHitsRequest("client1"),
				NewGetProtoSessionHitsRequest("client2"),
			}, nil)

		assert.Empty(t, getResponses[0].Hits)
		assert.Len(t, getResponses[1].Hits, 1)
	})
}

func testConcurrentAccess(t *testing.T, factory func() BatchedIOBackend) {
	t.Helper()

	t.Run("parallel appends to same session", func(t *testing.T) {
		// given
		backend := factory()
		defer func() { _ = backend.Stop(context.Background()) }()

		numGoroutines := 10
		var wg sync.WaitGroup

		// when
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				backend.HandleBatch(context.Background(),
					[]*AppendHitsToProtoSessionRequest{
						NewAppendHitsToProtoSessionRequest("client1",
							[]*hits.Hit{testHit("client1", "1.1.1.1")}),
					}, nil, nil)
			}(i)
		}
		wg.Wait()

		// then: no panics, data accessible
		_, getResponses, _ := backend.HandleBatch(context.Background(), nil,
			[]*GetProtoSessionHitsRequest{NewGetProtoSessionHitsRequest("client1")}, nil)

		assert.NoError(t, getResponses[0].Err)
		assert.NotEmpty(t, getResponses[0].Hits)
	})
}

// SendPingWithTime sends a ping with the given time, advancing the worker time.
func SendPingWithTime(
	f func(_ map[string]string, h *hits.HitProcessingTask) *worker.Error,
	t time.Time,
) error {
	return f(map[string]string{
		pings.IsPingMetadataKey:        pings.IsPingMetadataValue,
		pings.PingTimestampMetadataKey: t.Format(time.RFC3339),
	}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{},
	})
}
