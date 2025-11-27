package protosessionsv3

import (
	"bytes"
	"context"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
)

type naiveGenericStorageBatchedIOBackend struct {
	kv      storage.KV
	set     storage.Set
	encoder encoding.EncoderFunc
	decoder encoding.DecoderFunc
}

func identifierKey(identifierType string, extractor func(*hits.Hit) string, hit *hits.Hit) string {
	return fmt.Sprintf("ids.%s.%s", identifierType, extractor(hit))
}

func (b *naiveGenericStorageBatchedIOBackend) GetIdentifierConflicts(
	_ context.Context,
	requests []*IdentifierConflictRequest,
) []*IdentifierConflictResponse {
	results := make([]*IdentifierConflictResponse, len(requests))
	for i, request := range requests {
		result, err := b.kv.Set(
			[]byte(identifierKey(request.IdentifierType, request.ExtractIdentifier, request.Hit)),
			[]byte(request.Hit.AuthoritativeClientID),
			storage.WithSkipIfKeyAlreadyExists(true),
			storage.WithReturnPreviousValue(true),
		)
		if err != nil {
			results[i] = NewIdentifierConflictResponse(request, err, false, "")
		} else {
			if len(result) > 0 && string(result) != string(request.Hit.AuthoritativeClientID) {
				results[i] = NewIdentifierConflictResponse(request, nil, true, hits.ClientID(result))
			} else {
				results[i] = NewIdentifierConflictResponse(request, nil, false, "")
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
		appendResponses[i] = NewAppendHitsToProtoSessionResponse(err)
	}

	getResponses := make([]*GetProtoSessionHitsResponse, len(getProtoSessionHitsRequests))
	for i, request := range getProtoSessionHitsRequests {
		storageHits, err := b.set.All([]byte(protoSessionHitsKey(request.ProtoSessionID)))
		if err != nil {
			getResponses[i] = NewGetProtoSessionHitsResponse(nil, err)
			continue
		}
		allHits := make([]*hits.Hit, 0, len(storageHits))
		for _, hit := range storageHits {
			var decodedHit *hits.Hit
			err = b.decoder(bytes.NewBuffer(hit), &decodedHit)
			if err != nil {
				getResponses[i] = NewGetProtoSessionHitsResponse(nil, err)
				break
			}
			allHits = append(allHits, decodedHit)
		}
		if err == nil {
			getResponses[i] = NewGetProtoSessionHitsResponse(allHits, nil)
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
		markResponses[i] = NewMarkProtoSessionClosingForGivenBucketResponse(err)
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
			responses[i] = NewGetAllProtosessionsForBucketResponse(nil, err)
		} else {
			responses[i] = NewGetAllProtosessionsForBucketResponse(protoSessions, nil)
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

func (b *naiveGenericStorageBatchedIOBackend) Cleanup(
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
	for i, request := range hitsRequests {
		err := b.set.Drop([]byte(protoSessionHitsKey(request.ProtoSessionID)))
		hitsResponses[i] = NewRemoveProtoSessionHitsResponse(err)
	}
	metadataResponses := make([]*RemoveAllHitRelatedMetadataResponse, len(metadataRequests))
	for i, request := range metadataRequests {
		err := b.kv.Delete([]byte(identifierKey(request.IdentifierType, request.ExtractIdentifier, request.Hit)))
		metadataResponses[i] = NewRemoveAllHitRelatedMetadataResponse(err)
	}
	bucketMetadataResponses := make([]*RemoveBucketMetadataResponse, len(bucketMetadataRequests))
	for i, request := range bucketMetadataRequests {
		err := b.set.Drop([]byte(bucketsKey(request.BucketID)))
		bucketMetadataResponses[i] = NewRemoveBucketMetadataResponse(err)
	}
	return hitsResponses, metadataResponses, bucketMetadataResponses
}

func (b *naiveGenericStorageBatchedIOBackend) Stop(
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

func protoSessionHitsKey(authoritativeClientID hits.ClientID) string {
	return fmt.Sprintf("%s.%s", "sessions.hits", authoritativeClientID)
}

func bucketsKey(bucketNumber int64) string {
	return fmt.Sprintf("%s.%d", "sessions.buckets", bucketNumber)
}
