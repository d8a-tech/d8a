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

type AddToProtoSessionResult struct {
	Err error
}

type BatchedIOBackend interface {
	GetIdentifierConflicts(
		ctx context.Context,
		requests []*IdentifierConflictRequest,
	) []*IdentifierConflictResponse
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
