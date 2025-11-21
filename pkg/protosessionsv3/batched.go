package protosessionsv3

import (
	"context"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
)

type IdentifierConflictRequest struct {
	Hit               *hits.Hit
	IdentifierType    string
	ExtractIdentifier func(*hits.Hit) string
}

type IdentifierConflictResponse struct {
	Err           error
	HasConflict   bool
	ConflictsWith hits.ClientID
	Request       *IdentifierConflictRequest
}

type AppendHitsToProtoSessionRequest struct {
	ProtoSessionID hits.ClientID
	Hits           []*hits.Hit
}

type AppendHitsToProtoSessionResponse struct {
	Err error
}

type GetProtoSessionHitsRequest struct {
	ProtoSessionID hits.ClientID
}

type GetProtoSessionHitsResponse struct {
	Hits []*hits.Hit
	Err  error
}

type RemoveProtoSessionHitsRequest struct {
	ProtoSessionID hits.ClientID
}

type RemoveProtoSessionHitsResponse struct {
	Err error
}

type MarkProtoSessionClosingForGivenBucketRequest struct {
	ProtoSessionID hits.ClientID
	BucketID       int64
}

type MarkProtoSessionClosingForGivenBucketResponse struct {
	Err error
}

type GetAllProtosessionsForBucketRequest struct {
	BucketID int64
}

type GetAllProtosessionsForBucketResponse struct {
	ProtoSessions [][]*hits.Hit
	Err           error
}

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
	RemoveProtoSessionHits(
		ctx context.Context,
		requests []*RemoveProtoSessionHitsRequest,
	) []*RemoveProtoSessionHitsResponse
	// terminates all the gorutines, frees all the resources
	CleanupMachinery(
		ctx context.Context,
	) error
}

type naiveGenericStorageBatchedIOBackend struct {
	kv      storage.KV
	set     storage.Set
	encoder encoding.EncoderFunc
	decoder encoding.DecoderFunc
}

func (b *naiveGenericStorageBatchedIOBackend) GetIdentifierConflicts(
	_ context.Context,
	requests []*IdentifierConflictRequest,
) []*IdentifierConflictResponse {
	results := make([]*IdentifierConflictResponse, len(requests))
	for i, request := range requests {
		result, err := b.kv.Set(
			[]byte(fmt.Sprintf("ids.%s.%s", request.IdentifierType, request.ExtractIdentifier(request.Hit))),
			[]byte(request.Hit.AuthoritativeClientID),
			storage.WithSkipIfKeyAlreadyExists(true),
			storage.WithReturnPreviousValue(true),
		)
		if err != nil {
			results[i] = &IdentifierConflictResponse{
				Err: err,
			}
		} else {
			if len(result) > 0 && string(result) != string(request.Hit.AuthoritativeClientID) {
				results[i] = &IdentifierConflictResponse{
					Err:           nil,
					HasConflict:   true,
					ConflictsWith: hits.ClientID(result),
				}
			} else {
				results[i] = &IdentifierConflictResponse{
					Err:         nil,
					HasConflict: false,
				}
			}
		}
	}
	return results
}

func (b *naiveGenericStorageBatchedIOBackend) HandleBatch(
	ctx context.Context,
	appendHitsRequests []*AppendHitsToProtoSessionRequest,
	getProtoSessionHitsRequests []*GetProtoSessionHitsRequest,
	markProtoSessionClosingForGivenBucketRequests []*MarkProtoSessionClosingForGivenBucketRequest,
) (
	[]*AppendHitsToProtoSessionResponse,
	[]*GetProtoSessionHitsResponse,
	[]*MarkProtoSessionClosingForGivenBucketResponse,
) {
	return []*AppendHitsToProtoSessionResponse{}, []*GetProtoSessionHitsResponse{}, []*MarkProtoSessionClosingForGivenBucketResponse{}
}

func (b *naiveGenericStorageBatchedIOBackend) GetAllProtosessionsForBucket(
	ctx context.Context,
	requests []*GetAllProtosessionsForBucketRequest,
) []*GetAllProtosessionsForBucketResponse {
	return []*GetAllProtosessionsForBucketResponse{}
}

func (b *naiveGenericStorageBatchedIOBackend) RemoveProtoSessionHits(
	ctx context.Context,
	requests []*RemoveProtoSessionHitsRequest,
) []*RemoveProtoSessionHitsResponse {
	return []*RemoveProtoSessionHitsResponse{}
}

func (b *naiveGenericStorageBatchedIOBackend) CleanupMachinery(
	ctx context.Context,
) error {
	return nil
}

func NewNaiveGenericStorageBatchedIOBackend(
	kv storage.KV,
	set storage.Set,
	encoder encoding.EncoderFunc,
	decoder encoding.DecoderFunc,
) BatchedIOBackend {
	return &naiveGenericStorageBatchedIOBackend{
		kv:      kv,
		set:     set,
		encoder: encoder,
		decoder: decoder,
	}
}
