package protosessionsv3

import (
	"bytes"
	"context"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	bolt "go.etcd.io/bbolt"
)

const (
	identifiersBucket   = "identifiers"
	protoSessionsBucket = "protosessions"
	bucketsBucket       = "buckets"
)

type boltBatchedIOBackend struct {
	db      *bolt.DB
	encoder encoding.EncoderFunc
	decoder encoding.DecoderFunc
}

// NewBoltBatchedIOBackend creates a BatchedIOBackend using BoltDB with single-transaction batching
func NewBoltBatchedIOBackend(
	db *bolt.DB,
	encoder encoding.EncoderFunc,
	decoder encoding.DecoderFunc,
) (BatchedIOBackend, error) {
	// Ensure buckets exist
	err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(identifiersBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(protoSessionsBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(bucketsBucket)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &boltBatchedIOBackend{
		db:      db,
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (b *boltBatchedIOBackend) identifierKey(
	identifierType string,
	extractor func(*hits.Hit) string,
	hit *hits.Hit,
) []byte {
	return []byte(fmt.Sprintf("ids.%s.%s", identifierType, extractor(hit)))
}

func (b *boltBatchedIOBackend) protoSessionKey(clientID hits.ClientID) []byte {
	return []byte(fmt.Sprintf("sessions.hits.%s", clientID))
}

func (b *boltBatchedIOBackend) bucketKey(bucketID int64) []byte {
	return []byte(fmt.Sprintf("sessions.buckets.%d", bucketID))
}

// GetIdentifierConflicts processes all requests in a single transaction
func (b *boltBatchedIOBackend) GetIdentifierConflicts(
	_ context.Context,
	requests []*IdentifierConflictRequest,
) []*IdentifierConflictResponse {
	results := make([]*IdentifierConflictResponse, len(requests))

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(identifiersBucket))

		for i, request := range requests {
			key := b.identifierKey(request.IdentifierType, request.ExtractIdentifier, request.Hit)
			existingValue := bucket.Get(key)

			if existingValue == nil {
				// Key doesn't exist, register it
				if err := bucket.Put(key, []byte(request.Hit.AuthoritativeClientID)); err != nil {
					results[i] = NewIdentifierConflictResponse(request, err, false, "")
					continue
				}
				results[i] = NewIdentifierConflictResponse(request, nil, false, "")
			} else {
				// Key exists, check if it's the same client
				existingClient := string(existingValue)
				if existingClient == string(request.Hit.AuthoritativeClientID) {
					// Same client, no conflict
					results[i] = NewIdentifierConflictResponse(request, nil, false, "")
				} else {
					// Different client, conflict
					results[i] = NewIdentifierConflictResponse(request, nil, true, hits.ClientID(existingClient))
				}
			}
		}
		return nil
	})

	if err != nil {
		// If transaction failed, mark all as errors
		for i, request := range requests {
			if results[i] == nil {
				results[i] = NewIdentifierConflictResponse(request, err, false, "")
			}
		}
	}

	return results
}

// HandleBatch processes append, get, and mark operations in a single transaction
func (b *boltBatchedIOBackend) HandleBatch(
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
	getResponses := make([]*GetProtoSessionHitsResponse, len(getProtoSessionHitsRequests))
	markResponses := make([]*MarkProtoSessionClosingForGivenBucketResponse, len(markProtoSessionClosingForGivenBucketRequests))

	err := b.db.Update(func(tx *bolt.Tx) error {
		sessionsBucket := tx.Bucket([]byte(protoSessionsBucket))
		bucketsBucket := tx.Bucket([]byte(bucketsBucket))

		// Process append requests
		for i, request := range appendHitsRequests {
			key := b.protoSessionKey(request.ProtoSessionID)
			keyBucket, err := sessionsBucket.CreateBucketIfNotExists(key)
			if err != nil {
				appendResponses[i] = NewAppendHitsToProtoSessionResponse(err)
				continue
			}

			var appendErr error
			for _, hit := range request.Hits {
				buf := bytes.NewBuffer(nil)
				if _, encErr := b.encoder(buf, hit); encErr != nil {
					appendErr = encErr
					break
				}
				encoded := buf.Bytes()
				// Use encoded bytes as key (set semantics)
				if err := keyBucket.Put(encoded, []byte{1}); err != nil {
					appendErr = err
					break
				}
			}
			appendResponses[i] = NewAppendHitsToProtoSessionResponse(appendErr)
		}

		// Process get requests
		for i, request := range getProtoSessionHitsRequests {
			key := b.protoSessionKey(request.ProtoSessionID)
			keyBucket := sessionsBucket.Bucket(key)
			if keyBucket == nil {
				getResponses[i] = NewGetProtoSessionHitsResponse([]*hits.Hit{}, nil)
				continue
			}

			var allHits []*hits.Hit
			var getErr error
			err := keyBucket.ForEach(func(k, _ []byte) error {
				var decodedHit *hits.Hit
				if decErr := b.decoder(bytes.NewBuffer(k), &decodedHit); decErr != nil {
					getErr = decErr
					return decErr
				}
				allHits = append(allHits, decodedHit)
				return nil
			})
			if err != nil {
				getResponses[i] = NewGetProtoSessionHitsResponse(nil, getErr)
			} else {
				getResponses[i] = NewGetProtoSessionHitsResponse(allHits, nil)
			}
		}

		// Process mark closing requests
		for i, request := range markProtoSessionClosingForGivenBucketRequests {
			key := b.bucketKey(request.BucketID)
			keyBucket, err := bucketsBucket.CreateBucketIfNotExists(key)
			if err != nil {
				markResponses[i] = NewMarkProtoSessionClosingForGivenBucketResponse(err)
				continue
			}
			// Use client ID as key (set semantics - idempotent)
			if err := keyBucket.Put([]byte(request.ProtoSessionID), []byte{1}); err != nil {
				markResponses[i] = NewMarkProtoSessionClosingForGivenBucketResponse(err)
				continue
			}
			markResponses[i] = NewMarkProtoSessionClosingForGivenBucketResponse(nil)
		}

		return nil
	})

	if err != nil {
		// Fill in any nil responses with the transaction error
		for i := range appendHitsRequests {
			if appendResponses[i] == nil {
				appendResponses[i] = NewAppendHitsToProtoSessionResponse(err)
			}
		}
		for i := range getProtoSessionHitsRequests {
			if getResponses[i] == nil {
				getResponses[i] = NewGetProtoSessionHitsResponse(nil, err)
			}
		}
		for i := range markProtoSessionClosingForGivenBucketRequests {
			if markResponses[i] == nil {
				markResponses[i] = NewMarkProtoSessionClosingForGivenBucketResponse(err)
			}
		}
	}

	return appendResponses, getResponses, markResponses
}

// GetAllProtosessionsForBucket retrieves all proto-sessions for buckets in a single read transaction
func (b *boltBatchedIOBackend) GetAllProtosessionsForBucket(
	_ context.Context,
	requests []*GetAllProtosessionsForBucketRequest,
) []*GetAllProtosessionsForBucketResponse {
	responses := make([]*GetAllProtosessionsForBucketResponse, len(requests))

	err := b.db.View(func(tx *bolt.Tx) error {
		bucketsBucket := tx.Bucket([]byte(bucketsBucket))
		sessionsBucket := tx.Bucket([]byte(protoSessionsBucket))

		for i, request := range requests {
			bucketKey := b.bucketKey(request.BucketID)
			keyBucket := bucketsBucket.Bucket(bucketKey)
			if keyBucket == nil {
				responses[i] = NewGetAllProtosessionsForBucketResponse([][]*hits.Hit{}, nil)
				continue
			}

			var protoSessions [][]*hits.Hit
			var reqErr error

			err := keyBucket.ForEach(func(clientID, _ []byte) error {
				sessionKey := b.protoSessionKey(hits.ClientID(clientID))
				sessionBucket := sessionsBucket.Bucket(sessionKey)
				if sessionBucket == nil {
					protoSessions = append(protoSessions, []*hits.Hit{})
					return nil
				}

				var sessionHits []*hits.Hit
				err := sessionBucket.ForEach(func(encoded, _ []byte) error {
					var decodedHit *hits.Hit
					if decErr := b.decoder(bytes.NewBuffer(encoded), &decodedHit); decErr != nil {
						return decErr
					}
					sessionHits = append(sessionHits, decodedHit)
					return nil
				})
				if err != nil {
					reqErr = err
					return err
				}
				protoSessions = append(protoSessions, sessionHits)
				return nil
			})

			if err != nil {
				responses[i] = NewGetAllProtosessionsForBucketResponse(nil, reqErr)
			} else {
				responses[i] = NewGetAllProtosessionsForBucketResponse(protoSessions, nil)
			}
		}
		return nil
	})

	if err != nil {
		for i, request := range requests {
			if responses[i] == nil {
				responses[i] = NewGetAllProtosessionsForBucketResponse(nil, err)
				_ = request // silence unused warning
			}
		}
	}

	return responses
}

// Cleanup removes hits, metadata, and bucket data in a single transaction
func (b *boltBatchedIOBackend) Cleanup(
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
	metadataResponses := make([]*RemoveAllHitRelatedMetadataResponse, len(metadataRequests))
	bucketResponses := make([]*RemoveBucketMetadataResponse, len(bucketMetadataRequests))

	err := b.db.Update(func(tx *bolt.Tx) error {
		sessionsBucket := tx.Bucket([]byte(protoSessionsBucket))
		identifiersBucket := tx.Bucket([]byte(identifiersBucket))
		bucketsBucket := tx.Bucket([]byte(bucketsBucket))

		// Remove proto-session hits
		for i, request := range hitsRequests {
			key := b.protoSessionKey(request.ProtoSessionID)
			if sessionsBucket.Bucket(key) != nil {
				if err := sessionsBucket.DeleteBucket(key); err != nil {
					hitsResponses[i] = NewRemoveProtoSessionHitsResponse(err)
					continue
				}
			}
			hitsResponses[i] = NewRemoveProtoSessionHitsResponse(nil)
		}

		// Remove identifier metadata
		for i, request := range metadataRequests {
			key := b.identifierKey(request.IdentifierType, request.ExtractIdentifier, request.Hit)
			if err := identifiersBucket.Delete(key); err != nil {
				metadataResponses[i] = NewRemoveAllHitRelatedMetadataResponse(err)
				continue
			}
			metadataResponses[i] = NewRemoveAllHitRelatedMetadataResponse(nil)
		}

		// Remove bucket metadata
		for i, request := range bucketMetadataRequests {
			key := b.bucketKey(request.BucketID)
			if bucketsBucket.Bucket(key) != nil {
				if err := bucketsBucket.DeleteBucket(key); err != nil {
					bucketResponses[i] = NewRemoveBucketMetadataResponse(err)
					continue
				}
			}
			bucketResponses[i] = NewRemoveBucketMetadataResponse(nil)
		}

		return nil
	})

	if err != nil {
		for i := range hitsRequests {
			if hitsResponses[i] == nil {
				hitsResponses[i] = NewRemoveProtoSessionHitsResponse(err)
			}
		}
		for i := range metadataRequests {
			if metadataResponses[i] == nil {
				metadataResponses[i] = NewRemoveAllHitRelatedMetadataResponse(err)
			}
		}
		for i := range bucketMetadataRequests {
			if bucketResponses[i] == nil {
				bucketResponses[i] = NewRemoveBucketMetadataResponse(err)
			}
		}
	}

	return hitsResponses, metadataResponses, bucketResponses
}

// Stop closes the BoltDB connection
func (b *boltBatchedIOBackend) Stop(_ context.Context) error {
	return b.db.Close()
}
