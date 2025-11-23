package protosessionsv3

import (
	"context"

	"github.com/d8a-tech/d8a/pkg/hits"
)

// TestBatchedIOBackend is a configurable test implementation of BatchedIOBackend
type TestBatchedIOBackend struct {
	identifierConflictHandler                    func(*IdentifierConflictRequest) *IdentifierConflictResponse
	appendHitsHandler                            func(*AppendHitsToProtoSessionRequest) *AppendHitsToProtoSessionResponse
	getProtoSessionHitsHandler                   func(*GetProtoSessionHitsRequest) *GetProtoSessionHitsResponse
	markProtoSessionClosingForGivenBucketHandler func(*MarkProtoSessionClosingForGivenBucketRequest) *MarkProtoSessionClosingForGivenBucketResponse
	getAllProtosessionsForBucketHandler          func(*GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse
	removeProtoSessionHitsHandler                func(*RemoveProtoSessionHitsRequest) *RemoveProtoSessionHitsResponse
	cleanupMachineryHandler                      func() error
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

	markResponses := make([]*MarkProtoSessionClosingForGivenBucketResponse, len(markProtoSessionClosingForGivenBucketRequests))
	for i, req := range markProtoSessionClosingForGivenBucketRequests {
		markResponses[i] = t.markProtoSessionClosingForGivenBucketHandler(req)
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
		responses[i] = t.getAllProtosessionsForBucketHandler(req)
	}
	return responses
}

// RemoveProtoSessionEntities implements BatchedIOBackend
func (t *TestBatchedIOBackend) RemoveProtoSessionEntities(
	_ context.Context,
	requests []*RemoveProtoSessionHitsRequest,
) []*RemoveProtoSessionHitsResponse {
	responses := make([]*RemoveProtoSessionHitsResponse, len(requests))
	for i, req := range requests {
		responses[i] = t.removeProtoSessionHitsHandler(req)
	}
	return responses
}

// CleanupMachinery implements BatchedIOBackend
func (t *TestBatchedIOBackend) CleanupMachinery(_ context.Context) error {
	return t.cleanupMachineryHandler()
}

// TestBatchedIOBackendOption configures TestBatchedIOBackend
type TestBatchedIOBackendOption func(*TestBatchedIOBackend)

// WithIdentifierConflictHandler sets custom handler for identifier conflict requests
func WithIdentifierConflictHandler(
	handler func(*IdentifierConflictRequest) *IdentifierConflictResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.identifierConflictHandler = handler
	}
}

// WithAppendHitsHandler sets custom handler for append hits requests
func WithAppendHitsHandler(
	handler func(*AppendHitsToProtoSessionRequest) *AppendHitsToProtoSessionResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.appendHitsHandler = handler
	}
}

// WithGetProtoSessionHitsHandler sets custom handler for get proto session hits requests
func WithGetProtoSessionHitsHandler(
	handler func(*GetProtoSessionHitsRequest) *GetProtoSessionHitsResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.getProtoSessionHitsHandler = handler
	}
}

// WithMarkProtoSessionClosingHandler sets custom handler for mark closing requests
func WithMarkProtoSessionClosingHandler(
	handler func(*MarkProtoSessionClosingForGivenBucketRequest) *MarkProtoSessionClosingForGivenBucketResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.markProtoSessionClosingForGivenBucketHandler = handler
	}
}

// WithGetAllProtosessionsForBucketHandler sets custom handler for bucket proto sessions requests
func WithGetAllProtosessionsForBucketHandler(
	handler func(*GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.getAllProtosessionsForBucketHandler = handler
	}
}

// WithRemoveProtoSessionHitsHandler sets custom handler for remove hits requests
func WithRemoveProtoSessionHitsHandler(
	handler func(*RemoveProtoSessionHitsRequest) *RemoveProtoSessionHitsResponse,
) TestBatchedIOBackendOption {
	return func(b *TestBatchedIOBackend) {
		b.removeProtoSessionHitsHandler = handler
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
		markProtoSessionClosingForGivenBucketHandler: func(_ *MarkProtoSessionClosingForGivenBucketRequest) *MarkProtoSessionClosingForGivenBucketResponse {
			return &MarkProtoSessionClosingForGivenBucketResponse{
				Err: nil,
			}
		},
		getAllProtosessionsForBucketHandler: func(_ *GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse {
			return &GetAllProtosessionsForBucketResponse{
				ProtoSessions: [][]*hits.Hit{},
				Err:           nil,
			}
		},
		removeProtoSessionHitsHandler: func(_ *RemoveProtoSessionHitsRequest) *RemoveProtoSessionHitsResponse {
			return &RemoveProtoSessionHitsResponse{
				Err: nil,
			}
		},
		cleanupMachineryHandler: func() error {
			return nil
		},
	}

	for _, opt := range opts {
		opt(backend)
	}

	return backend
}

// TestCloser is a configurable test implementation of Closer
type TestCloser struct {
	closeHandler func([]*hits.Hit) error
}

// Close implements Closer
func (c *TestCloser) Close(protosession []*hits.Hit) error {
	return c.closeHandler(protosession)
}

// TestCloserOption configures TestCloser
type TestCloserOption func(*TestCloser)

// WithCloseHandler sets custom handler for closing proto-sessions
func WithCloseHandler(handler func([]*hits.Hit) error) TestCloserOption {
	return func(c *TestCloser) {
		c.closeHandler = handler
	}
}

// NewTestCloser creates a test closer with success-like defaults
func NewTestCloser(opts ...TestCloserOption) Closer {
	closer := &TestCloser{
		closeHandler: func(_ []*hits.Hit) error {
			return nil
		},
	}

	for _, opt := range opts {
		opt(closer)
	}

	return closer
}
