package protosessionsv3

import (
	"context"

	"github.com/d8a-tech/d8a/pkg/hits"
)

// IdentifierConflictRequest represents a request to check for identifier conflicts
type IdentifierConflictRequest struct {
	Hit               *hits.Hit
	IdentifierType    string
	ExtractIdentifier func(*hits.Hit) string
}

// NewIdentifierConflictRequest creates a new identifier conflict request
func NewIdentifierConflictRequest(
	hit *hits.Hit,
	identifierType string,
	extractIdentifier func(*hits.Hit) string,
) *IdentifierConflictRequest {
	return &IdentifierConflictRequest{
		Hit:               hit,
		IdentifierType:    identifierType,
		ExtractIdentifier: extractIdentifier,
	}
}

// IdentifierConflictResponse represents the result of identifier conflict check
type IdentifierConflictResponse struct {
	Err           error
	HasConflict   bool
	ConflictsWith hits.ClientID
	Request       *IdentifierConflictRequest
}

// NewIdentifierConflictResponse creates a new identifier conflict response
func NewIdentifierConflictResponse(
	request *IdentifierConflictRequest,
	err error,
	hasConflict bool,
	conflictsWith hits.ClientID,
) *IdentifierConflictResponse {
	return &IdentifierConflictResponse{
		Request:       request,
		Err:           err,
		HasConflict:   hasConflict,
		ConflictsWith: conflictsWith,
	}
}

// AppendHitsToProtoSessionRequest represents a request to append hits to a proto-session
type AppendHitsToProtoSessionRequest struct {
	ProtoSessionID hits.ClientID
	Hits           []*hits.Hit
}

// NewAppendHitsToProtoSessionRequest creates a new append hits request
func NewAppendHitsToProtoSessionRequest(
	protoSessionID hits.ClientID,
	hits []*hits.Hit,
) *AppendHitsToProtoSessionRequest {
	return &AppendHitsToProtoSessionRequest{
		ProtoSessionID: protoSessionID,
		Hits:           hits,
	}
}

// AppendHitsToProtoSessionResponse represents the result of appending hits
type AppendHitsToProtoSessionResponse struct {
	Err error
}

// NewAppendHitsToProtoSessionResponse creates a new append hits response
func NewAppendHitsToProtoSessionResponse(err error) *AppendHitsToProtoSessionResponse {
	return &AppendHitsToProtoSessionResponse{Err: err}
}

// GetProtoSessionHitsRequest represents a request to get hits for a proto-session
type GetProtoSessionHitsRequest struct {
	ProtoSessionID hits.ClientID
}

// NewGetProtoSessionHitsRequest creates a new get proto-session hits request
func NewGetProtoSessionHitsRequest(protoSessionID hits.ClientID) *GetProtoSessionHitsRequest {
	return &GetProtoSessionHitsRequest{ProtoSessionID: protoSessionID}
}

// GetProtoSessionHitsResponse represents the result of getting proto-session hits
type GetProtoSessionHitsResponse struct {
	Hits []*hits.Hit
	Err  error
}

// NewGetProtoSessionHitsResponse creates a new get proto-session hits response
func NewGetProtoSessionHitsResponse(hits []*hits.Hit, err error) *GetProtoSessionHitsResponse {
	return &GetProtoSessionHitsResponse{Hits: hits, Err: err}
}

// RemoveProtoSessionHitsRequest represents a request to remove proto-session hits
type RemoveProtoSessionHitsRequest struct {
	ProtoSessionID hits.ClientID
}

// NewRemoveProtoSessionHitsRequest creates a new remove proto-session hits request
func NewRemoveProtoSessionHitsRequest(protoSessionID hits.ClientID) *RemoveProtoSessionHitsRequest {
	return &RemoveProtoSessionHitsRequest{ProtoSessionID: protoSessionID}
}

// RemoveProtoSessionHitsResponse represents the result of removing proto-session hits
type RemoveProtoSessionHitsResponse struct {
	Err error
}

// NewRemoveProtoSessionHitsResponse creates a new remove proto-session hits response
func NewRemoveProtoSessionHitsResponse(err error) *RemoveProtoSessionHitsResponse {
	return &RemoveProtoSessionHitsResponse{Err: err}
}

// RemoveAllHitRelatedMetadataRequest represents a request to remove all hit-related metadata
type RemoveAllHitRelatedMetadataRequest struct {
	IdentifierType    string
	ExtractIdentifier func(*hits.Hit) string
	Hit               *hits.Hit
}

// NewRemoveAllHitRelatedMetadataRequest creates a new remove hit-related metadata request
func NewRemoveAllHitRelatedMetadataRequest(
	hit *hits.Hit,
	identifierType string,
	extractIdentifier func(*hits.Hit) string,
) *RemoveAllHitRelatedMetadataRequest {
	return &RemoveAllHitRelatedMetadataRequest{
		Hit:               hit,
		IdentifierType:    identifierType,
		ExtractIdentifier: extractIdentifier,
	}
}

// RemoveAllHitRelatedMetadataResponse represents the result of removing all hit-related metadata
type RemoveAllHitRelatedMetadataResponse struct {
	Err error
}

// NewRemoveAllHitRelatedMetadataResponse creates a new remove hit-related metadata response
func NewRemoveAllHitRelatedMetadataResponse(err error) *RemoveAllHitRelatedMetadataResponse {
	return &RemoveAllHitRelatedMetadataResponse{Err: err}
}

// RemoveBucketMetadataRequest represents a request to remove bucket metadata
type RemoveBucketMetadataRequest struct {
	BucketID int64
}

// NewRemoveBucketMetadataRequest creates a new remove bucket metadata request
func NewRemoveBucketMetadataRequest(bucketID int64) *RemoveBucketMetadataRequest {
	return &RemoveBucketMetadataRequest{BucketID: bucketID}
}

// RemoveBucketMetadataResponse represents the result of removing bucket metadata
type RemoveBucketMetadataResponse struct {
	Err error
}

// NewRemoveBucketMetadataResponse creates a new remove bucket metadata response
func NewRemoveBucketMetadataResponse(err error) *RemoveBucketMetadataResponse {
	return &RemoveBucketMetadataResponse{Err: err}
}

// MarkProtoSessionClosingForGivenBucketRequest marks a proto-session for closing in a bucket
type MarkProtoSessionClosingForGivenBucketRequest struct {
	ProtoSessionID hits.ClientID
	BucketID       int64
}

// NewMarkProtoSessionClosingForGivenBucketRequest creates a new mark closing request
func NewMarkProtoSessionClosingForGivenBucketRequest(
	protoSessionID hits.ClientID,
	bucketID int64,
) *MarkProtoSessionClosingForGivenBucketRequest {
	return &MarkProtoSessionClosingForGivenBucketRequest{
		ProtoSessionID: protoSessionID,
		BucketID:       bucketID,
	}
}

// MarkProtoSessionClosingForGivenBucketResponse represents the result of marking closing
type MarkProtoSessionClosingForGivenBucketResponse struct {
	Err error
}

// NewMarkProtoSessionClosingForGivenBucketResponse creates a new mark closing response
func NewMarkProtoSessionClosingForGivenBucketResponse(err error) *MarkProtoSessionClosingForGivenBucketResponse {
	return &MarkProtoSessionClosingForGivenBucketResponse{Err: err}
}

// GetAllProtosessionsForBucketRequest represents a request to get all proto-sessions for a bucket
type GetAllProtosessionsForBucketRequest struct {
	BucketID int64
}

// NewGetAllProtosessionsForBucketRequest creates a new get all protosessions for bucket request
func NewGetAllProtosessionsForBucketRequest(bucketID int64) *GetAllProtosessionsForBucketRequest {
	return &GetAllProtosessionsForBucketRequest{BucketID: bucketID}
}

// GetAllProtosessionsForBucketResponse represents all proto-sessions for a bucket
type GetAllProtosessionsForBucketResponse struct {
	ProtoSessions [][]*hits.Hit
	Err           error
}

// NewGetAllProtosessionsForBucketResponse creates a new get all protosessions for bucket response
func NewGetAllProtosessionsForBucketResponse(
	protoSessions [][]*hits.Hit,
	err error,
) *GetAllProtosessionsForBucketResponse {
	return &GetAllProtosessionsForBucketResponse{
		ProtoSessions: protoSessions,
		Err:           err,
	}
}

// BatchedIOBackend provides batched I/O operations for proto-sessions
type BatchedIOBackend interface {
	GetIdentifierConflicts(
		ctx context.Context,
		requests []*IdentifierConflictRequest,
	) []*IdentifierConflictResponse
	HandleBatch(
		ctx context.Context,
		appendHitsRequests []*AppendHitsToProtoSessionRequest,
		getProtoSessionHitsRequests []*GetProtoSessionHitsRequest,
		markProtoSessionClosingForGivenBucketRequests []*MarkProtoSessionClosingForGivenBucketRequest,
	) (
		[]*AppendHitsToProtoSessionResponse,
		[]*GetProtoSessionHitsResponse,
		[]*MarkProtoSessionClosingForGivenBucketResponse,
	)
	GetAllProtosessionsForBucket(
		ctx context.Context,
		requests []*GetAllProtosessionsForBucketRequest,
	) []*GetAllProtosessionsForBucketResponse
	Cleanup(
		ctx context.Context,
		hitsRequests []*RemoveProtoSessionHitsRequest,
		metadataRequests []*RemoveAllHitRelatedMetadataRequest,
		bucketMetadataRequests []*RemoveBucketMetadataRequest,
	) (
		[]*RemoveProtoSessionHitsResponse,
		[]*RemoveAllHitRelatedMetadataResponse,
		[]*RemoveBucketMetadataResponse,
	)
	// Stops the backend, terminates all the gorutines, frees all the resources
	Stop(context.Context) error
}
