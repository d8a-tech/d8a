package protosessionsv3

import (
	"bytes"
	"context"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
)

// IdentifierConflictRequest represents a request to check for identifier conflicts
type IdentifierConflictRequest struct {
	Hit               *hits.Hit
	IdentifierType    string
	ExtractIdentifier func(*hits.Hit) string
}

// IdentifierConflictResponse represents the result of identifier conflict check
type IdentifierConflictResponse struct {
	Err           error
	HasConflict   bool
	ConflictsWith hits.ClientID
	Request       *IdentifierConflictRequest
}

// AppendHitsToProtoSessionRequest represents a request to append hits to a proto-session
type AppendHitsToProtoSessionRequest struct {
	ProtoSessionID hits.ClientID
	Hits           []*hits.Hit
}

// AppendHitsToProtoSessionResponse represents the result of appending hits
type AppendHitsToProtoSessionResponse struct {
	Err error
}

// GetProtoSessionHitsRequest represents a request to get hits for a proto-session
type GetProtoSessionHitsRequest struct {
	ProtoSessionID hits.ClientID
}

// GetProtoSessionHitsResponse represents the result of getting proto-session hits
type GetProtoSessionHitsResponse struct {
	Hits []*hits.Hit
	Err  error
}

// RemoveProtoSessionHitsRequest represents a request to remove proto-session hits
type RemoveProtoSessionHitsRequest struct {
	ProtoSessionID hits.ClientID
}

// RemoveProtoSessionHitsResponse represents the result of removing proto-session hits
type RemoveProtoSessionHitsResponse struct {
	Err error
}

// RemoveAllHitRelatedMetadataRequest represents a request to remove all hit-related metadata
type RemoveAllHitRelatedMetadataRequest struct {
	hit *hits.Hit
}

// RemoveAllHitRelatedMetadataResponse represents the result of removing all hit-related metadata
type RemoveAllHitRelatedMetadataResponse struct {
	Err error
}

// MarkProtoSessionClosingForGivenBucketRequest marks a proto-session for closing in a bucket
type MarkProtoSessionClosingForGivenBucketRequest struct {
	ProtoSessionID hits.ClientID
	BucketID       int64
}

// MarkProtoSessionClosingForGivenBucketResponse represents the result of marking closing
type MarkProtoSessionClosingForGivenBucketResponse struct {
	Err error
}

// GetAllProtosessionsForBucketRequest represents a request to get all proto-sessions for a bucket
type GetAllProtosessionsForBucketRequest struct {
	BucketID int64
}

// GetAllProtosessionsForBucketResponse represents all proto-sessions for a bucket
type GetAllProtosessionsForBucketResponse struct {
	ProtoSessions [][]*hits.Hit
	Err           error
}

// BatchedIOBackend provides batched I/O operations for proto-sessions
type BatchedIOBackend interface {
	// TODO cleanup the identifiers - how?
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
	RemoveProtoSessionEntities(
		ctx context.Context,
		hitsRequests []*RemoveProtoSessionHitsRequest,
		metadataRequests []*RemoveAllHitRelatedMetadataRequest,
	) (
		[]*RemoveProtoSessionHitsResponse,
		[]*RemoveAllHitRelatedMetadataResponse,
	)
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
	for i, request := range appendHitsRequests {
		var err error
		for _, hit := range request.Hits {
			buf := bytes.NewBuffer(nil)
			_, encodeErr := b.encoder(buf, hit)
			if encodeErr != nil {
				err = encodeErr
				break
			}
			addErr := b.set.Add([]byte(protoSessionHitsKey(request.ProtoSessionID)), buf.Bytes())
			if addErr != nil {
				err = addErr
				break
			}
		}
		appendResponses[i] = &AppendHitsToProtoSessionResponse{Err: err}
	}

	getResponses := make([]*GetProtoSessionHitsResponse, len(getProtoSessionHitsRequests))
	for i, request := range getProtoSessionHitsRequests {
		storageHits, err := b.set.All([]byte(protoSessionHitsKey(request.ProtoSessionID)))
		if err != nil {
			getResponses[i] = &GetProtoSessionHitsResponse{Err: err}
			continue
		}
		allHits := make([]*hits.Hit, 0, len(storageHits))
		for _, hit := range storageHits {
			var decodedHit *hits.Hit
			err = b.decoder(bytes.NewBuffer(hit), &decodedHit)
			if err != nil {
				getResponses[i] = &GetProtoSessionHitsResponse{Err: err}
				break
			}
			allHits = append(allHits, decodedHit)
		}
		if err == nil {
			getResponses[i] = &GetProtoSessionHitsResponse{Hits: allHits}
		}
	}

	markResponses := make(
		[]*MarkProtoSessionClosingForGivenBucketResponse,
		len(markProtoSessionClosingForGivenBucketRequests),
	)
	for i, request := range markProtoSessionClosingForGivenBucketRequests {
		err := b.set.Add(
			[]byte(bucketsKey(request.BucketID)),
			[]byte(request.ProtoSessionID),
		)
		markResponses[i] = &MarkProtoSessionClosingForGivenBucketResponse{Err: err}
	}

	return appendResponses, getResponses, markResponses
}

func (b *naiveGenericStorageBatchedIOBackend) GetAllProtosessionsForBucket(
	_ context.Context,
	requests []*GetAllProtosessionsForBucketRequest,
) []*GetAllProtosessionsForBucketResponse {
	responses := make([]*GetAllProtosessionsForBucketResponse, len(requests))
	for i, request := range requests {
		protoSessions, err := b.getAllProtosessionsForSingleBucket(request.BucketID)
		if err != nil {
			responses[i] = &GetAllProtosessionsForBucketResponse{Err: err}
		} else {
			responses[i] = &GetAllProtosessionsForBucketResponse{ProtoSessions: protoSessions}
		}
	}
	return responses
}

func (b *naiveGenericStorageBatchedIOBackend) getAllProtosessionsForSingleBucket(
	bucketID int64,
) ([][]*hits.Hit, error) {
	allAuthoritativeClientIDs, err := b.set.All([]byte(bucketsKey(bucketID)))
	if err != nil {
		return nil, err
	}

	protoSessions := make([][]*hits.Hit, 0, len(allAuthoritativeClientIDs))
	for _, authoritativeClientID := range allAuthoritativeClientIDs {
		storageHits, err := b.set.All([]byte(protoSessionHitsKey(hits.ClientID(authoritativeClientID))))
		if err != nil {
			return nil, err
		}

		sessionHits := make([]*hits.Hit, 0, len(storageHits))
		for _, hit := range storageHits {
			var decodedHit *hits.Hit
			err = b.decoder(bytes.NewBuffer(hit), &decodedHit)
			if err != nil {
				return nil, err
			}
			sessionHits = append(sessionHits, decodedHit)
		}
		protoSessions = append(protoSessions, sessionHits)
	}
	return protoSessions, nil
}

func (b *naiveGenericStorageBatchedIOBackend) RemoveProtoSessionEntities(
	_ context.Context,
	hitsRequests []*RemoveProtoSessionHitsRequest,
	metadataRequests []*RemoveAllHitRelatedMetadataRequest,
) ([]*RemoveProtoSessionHitsResponse, []*RemoveAllHitRelatedMetadataResponse) {
	hitsResponses := make([]*RemoveProtoSessionHitsResponse, len(hitsRequests))
	for i, request := range hitsRequests {
		err := b.set.Drop([]byte(protoSessionHitsKey(request.ProtoSessionID)))
		hitsResponses[i] = &RemoveProtoSessionHitsResponse{Err: err}
	}
	// TODO: Remove the session stamp mappings here
	metadataResponses := make([]*RemoveAllHitRelatedMetadataResponse, len(metadataRequests))
	for i, request := range metadataRequests {
		err := b.kv.Delete([]byte(fmt.Sprintf("metadata.%s.%s", request.hit.AuthoritativeClientID, request.hit.SessionStamp())))
		metadataResponses[i] = &RemoveAllHitRelatedMetadataResponse{Err: err}
	}
	return hitsResponses, metadataResponses
}

func (b *naiveGenericStorageBatchedIOBackend) CleanupMachinery(
	_ context.Context,
) error {
	return nil
}

// NewNaiveGenericStorageBatchedIOBackend creates a batched I/O backend using generic storage
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

const protoSessionHitsPrefix = "sessions.hits"

func protoSessionHitsKey(authoritativeClientID hits.ClientID) string {
	return fmt.Sprintf("%s.%s", protoSessionHitsPrefix, authoritativeClientID)
}

const bucketsPrefix = "sessions.buckets"

func bucketsKey(bucketNumber int64) string {
	return fmt.Sprintf("%s.%d", bucketsPrefix, bucketNumber)
}
