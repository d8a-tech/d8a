package bolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protosessionsv3"
	bolt "go.etcd.io/bbolt"
)

const (
	identifiersBucket        = "identifiers"
	protoSessionsBucket      = "protosessions"
	bucketsBucket            = "buckets"
	sessionToBucketMapBucket = "sessionToBucket"
)

type boltBatchedIOBackend struct {
	db      *bolt.DB
	encoder encoding.EncoderFunc
	decoder encoding.DecoderFunc

	sessionToBucketMu  sync.RWMutex
	sessionToBucketMap map[hits.ClientID]int64
}

// NewBatchedProtosessionsIOBackend creates a BatchedIOBackend using BoltDB with single-transaction batching
func NewBatchedProtosessionsIOBackend(
	db *bolt.DB,
	encoder encoding.EncoderFunc,
	decoder encoding.DecoderFunc,
) (protosessionsv3.BatchedIOBackend, error) {
	sessionToBucketMap := make(map[hits.ClientID]int64)

	// Ensure buckets exist and load session-to-bucket map
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
		mapBucket, err := tx.CreateBucketIfNotExists([]byte(sessionToBucketMapBucket))
		if err != nil {
			return err
		}
		// Load existing session-to-bucket mappings into memory
		return mapBucket.ForEach(func(k, v []byte) error {
			//nolint:gosec // bucket IDs are always positive
			sessionToBucketMap[hits.ClientID(k)] = int64(binary.BigEndian.Uint64(v))
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return &boltBatchedIOBackend{
		db:                 db,
		encoder:            encoder,
		decoder:            decoder,
		sessionToBucketMap: sessionToBucketMap,
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
	requests []*protosessionsv3.IdentifierConflictRequest,
) []*protosessionsv3.IdentifierConflictResponse {
	results := make([]*protosessionsv3.IdentifierConflictResponse, len(requests))

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(identifiersBucket))

		for i, request := range requests {
			key := b.identifierKey(request.IdentifierType, request.ExtractIdentifier, request.Hit)
			existingValue := bucket.Get(key)

			if existingValue == nil {
				// Key doesn't exist, register it
				if err := bucket.Put(key, []byte(request.Hit.AuthoritativeClientID)); err != nil {
					results[i] = protosessionsv3.NewIdentifierConflictResponse(request, err, false, "")
					continue
				}
				results[i] = protosessionsv3.NewIdentifierConflictResponse(request, nil, false, "")
			} else {
				// Key exists, check if it's the same client
				existingClient := string(existingValue)
				if existingClient == string(request.Hit.AuthoritativeClientID) {
					// Same client, no conflict
					results[i] = protosessionsv3.NewIdentifierConflictResponse(request, nil, false, "")
				} else {
					// Different client, conflict
					results[i] = protosessionsv3.NewIdentifierConflictResponse(request, nil, true, hits.ClientID(existingClient))
				}
			}
		}
		return nil
	})

	if err != nil {
		// If transaction failed, mark all as errors
		for i, request := range requests {
			if results[i] == nil {
				results[i] = protosessionsv3.NewIdentifierConflictResponse(request, err, false, "")
			}
		}
	}

	return results
}

func (b *boltBatchedIOBackend) processAppendRequests(
	sessionsBucket *bolt.Bucket,
	requests []*protosessionsv3.AppendHitsToProtoSessionRequest,
	responses []*protosessionsv3.AppendHitsToProtoSessionResponse,
) {
	for i, request := range requests {
		key := b.protoSessionKey(request.ProtoSessionID)
		keyBucket, err := sessionsBucket.CreateBucketIfNotExists(key)
		if err != nil {
			responses[i] = protosessionsv3.NewAppendHitsToProtoSessionResponse(err)
			continue
		}
		responses[i] = protosessionsv3.NewAppendHitsToProtoSessionResponse(b.appendHitsToBucket(keyBucket, request.Hits))
	}
}

func (b *boltBatchedIOBackend) appendHitsToBucket(bucket *bolt.Bucket, hitsToAppend []*hits.Hit) error {
	for _, hit := range hitsToAppend {
		buf := bytes.NewBuffer(nil)
		if _, encErr := b.encoder(buf, hit); encErr != nil {
			return encErr
		}
		if err := bucket.Put(buf.Bytes(), []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

func (b *boltBatchedIOBackend) processGetRequests(
	sessionsBucket *bolt.Bucket,
	requests []*protosessionsv3.GetProtoSessionHitsRequest,
	responses []*protosessionsv3.GetProtoSessionHitsResponse,
) {
	for i, request := range requests {
		key := b.protoSessionKey(request.ProtoSessionID)
		keyBucket := sessionsBucket.Bucket(key)
		if keyBucket == nil {
			responses[i] = protosessionsv3.NewGetProtoSessionHitsResponse([]*hits.Hit{}, nil)
			continue
		}
		allHits, err := b.decodeHitsFromBucket(keyBucket)
		responses[i] = protosessionsv3.NewGetProtoSessionHitsResponse(allHits, err)
	}
}

func (b *boltBatchedIOBackend) decodeHitsFromBucket(bucket *bolt.Bucket) ([]*hits.Hit, error) {
	var allHits []*hits.Hit
	err := bucket.ForEach(func(k, _ []byte) error {
		var decodedHit *hits.Hit
		if decErr := b.decoder(bytes.NewBuffer(k), &decodedHit); decErr != nil {
			return decErr
		}
		allHits = append(allHits, decodedHit)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return allHits, nil
}

func (b *boltBatchedIOBackend) processMarkRequests(
	bucketsBucket *bolt.Bucket,
	mapBucket *bolt.Bucket,
	requests []*protosessionsv3.MarkProtoSessionClosingForGivenBucketRequest,
	responses []*protosessionsv3.MarkProtoSessionClosingForGivenBucketResponse,
	mapUpdates map[hits.ClientID]int64,
) {
	for i, request := range requests {
		key := b.bucketKey(request.BucketID)
		keyBucket, err := bucketsBucket.CreateBucketIfNotExists(key)
		if err != nil {
			responses[i] = protosessionsv3.NewMarkProtoSessionClosingForGivenBucketResponse(err)
			continue
		}
		if err := keyBucket.Put([]byte(request.ProtoSessionID), []byte{1}); err != nil {
			responses[i] = protosessionsv3.NewMarkProtoSessionClosingForGivenBucketResponse(err)
			continue
		}
		// Persist session-to-bucket mapping
		bucketIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bucketIDBytes, uint64(request.BucketID)) //nolint:gosec // bucket IDs are always positive
		if err := mapBucket.Put([]byte(request.ProtoSessionID), bucketIDBytes); err != nil {
			responses[i] = protosessionsv3.NewMarkProtoSessionClosingForGivenBucketResponse(err)
			continue
		}
		// Collect for in-memory map update (applied after transaction commits)
		mapUpdates[request.ProtoSessionID] = request.BucketID
		responses[i] = protosessionsv3.NewMarkProtoSessionClosingForGivenBucketResponse(nil)
	}
}

// HandleBatch processes append, get, and mark operations in a single transaction
func (b *boltBatchedIOBackend) HandleBatch(
	_ context.Context,
	appendHitsRequests []*protosessionsv3.AppendHitsToProtoSessionRequest,
	getProtoSessionHitsRequests []*protosessionsv3.GetProtoSessionHitsRequest,
	markProtoSessionClosingForGivenBucketRequests []*protosessionsv3.MarkProtoSessionClosingForGivenBucketRequest,
) (
	[]*protosessionsv3.AppendHitsToProtoSessionResponse,
	[]*protosessionsv3.GetProtoSessionHitsResponse,
	[]*protosessionsv3.MarkProtoSessionClosingForGivenBucketResponse,
) {
	appendResponses := make([]*protosessionsv3.AppendHitsToProtoSessionResponse, len(appendHitsRequests))
	getResponses := make([]*protosessionsv3.GetProtoSessionHitsResponse, len(getProtoSessionHitsRequests))
	markReqs := markProtoSessionClosingForGivenBucketRequests
	markResponses := make([]*protosessionsv3.MarkProtoSessionClosingForGivenBucketResponse, len(markReqs))
	mapUpdates := make(map[hits.ClientID]int64)

	err := b.db.Update(func(tx *bolt.Tx) error {
		sessionsBucket := tx.Bucket([]byte(protoSessionsBucket))
		bucketsBucket := tx.Bucket([]byte(bucketsBucket))
		mapBucket := tx.Bucket([]byte(sessionToBucketMapBucket))

		b.processAppendRequests(sessionsBucket, appendHitsRequests, appendResponses)
		b.processGetRequests(sessionsBucket, getProtoSessionHitsRequests, getResponses)
		b.processMarkRequests(bucketsBucket, mapBucket, markReqs, markResponses, mapUpdates)

		return nil
	})

	if err != nil {
		fillNilAppendResponses(appendResponses, err)
		fillNilGetResponses(getResponses, err)
		fillNilMarkResponses(markResponses, err)
	} else {
		// Apply map updates after successful transaction
		b.sessionToBucketMu.Lock()
		for sessionID, bucketID := range mapUpdates {
			b.sessionToBucketMap[sessionID] = bucketID
		}
		b.sessionToBucketMu.Unlock()
	}

	return appendResponses, getResponses, markResponses
}

func fillNilAppendResponses(responses []*protosessionsv3.AppendHitsToProtoSessionResponse, err error) {
	for i := range responses {
		if responses[i] == nil {
			responses[i] = protosessionsv3.NewAppendHitsToProtoSessionResponse(err)
		}
	}
}

func fillNilGetResponses(responses []*protosessionsv3.GetProtoSessionHitsResponse, err error) {
	for i := range responses {
		if responses[i] == nil {
			responses[i] = protosessionsv3.NewGetProtoSessionHitsResponse(nil, err)
		}
	}
}

func fillNilMarkResponses(responses []*protosessionsv3.MarkProtoSessionClosingForGivenBucketResponse, err error) {
	for i := range responses {
		if responses[i] == nil {
			responses[i] = protosessionsv3.NewMarkProtoSessionClosingForGivenBucketResponse(err)
		}
	}
}

// GetAllProtosessionsForBucket retrieves all proto-sessions for buckets in a single read transaction.
// Only returns sessions whose latest bucket (from the in-memory map) matches the requested bucket.
func (b *boltBatchedIOBackend) GetAllProtosessionsForBucket(
	_ context.Context,
	requests []*protosessionsv3.GetAllProtosessionsForBucketRequest,
) []*protosessionsv3.GetAllProtosessionsForBucketResponse {
	responses := make([]*protosessionsv3.GetAllProtosessionsForBucketResponse, len(requests))
	// Take a snapshot of the map under read lock
	b.sessionToBucketMu.RLock()
	mapSnapshot := make(map[hits.ClientID]int64, len(b.sessionToBucketMap))
	for k, v := range b.sessionToBucketMap {
		mapSnapshot[k] = v
	}
	b.sessionToBucketMu.RUnlock()
	err := b.db.View(func(tx *bolt.Tx) error {
		bucketsBucket := tx.Bucket([]byte(bucketsBucket))
		sessionsBucket := tx.Bucket([]byte(protoSessionsBucket))
		for i, request := range requests {
			bucketKey := b.bucketKey(request.BucketID)
			keyBucket := bucketsBucket.Bucket(bucketKey)
			if keyBucket == nil {
				responses[i] = protosessionsv3.NewGetAllProtosessionsForBucketResponse([][]*hits.Hit{}, nil)
				continue
			}
			var protoSessions [][]*hits.Hit
			var reqErr error
			err := keyBucket.ForEach(func(clientID, _ []byte) error {
				sessionID := hits.ClientID(clientID)
				// Filter: only process if this bucket is the session's latest bucket
				if latestBucket, ok := mapSnapshot[sessionID]; !ok || latestBucket != request.BucketID {
					return nil
				}
				sessionKey := b.protoSessionKey(sessionID)
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
				responses[i] = protosessionsv3.NewGetAllProtosessionsForBucketResponse(nil, reqErr)
			} else {
				responses[i] = protosessionsv3.NewGetAllProtosessionsForBucketResponse(protoSessions, nil)
			}
		}
		return nil
	})
	if err != nil {
		for i := range requests {
			if responses[i] == nil {
				responses[i] = protosessionsv3.NewGetAllProtosessionsForBucketResponse(nil, err)
			}
		}
	}
	return responses
}

func (b *boltBatchedIOBackend) removeProtoSessionHits(
	sessionsBucket *bolt.Bucket,
	mapBucket *bolt.Bucket,
	requests []*protosessionsv3.RemoveProtoSessionHitsRequest,
	responses []*protosessionsv3.RemoveProtoSessionHitsResponse,
	mapDeletions []hits.ClientID,
) []hits.ClientID {
	for i, request := range requests {
		key := b.protoSessionKey(request.ProtoSessionID)
		if sessionsBucket.Bucket(key) != nil {
			if err := sessionsBucket.DeleteBucket(key); err != nil {
				responses[i] = protosessionsv3.NewRemoveProtoSessionHitsResponse(err)
				continue
			}
		}
		// Remove from persisted session-to-bucket map
		if err := mapBucket.Delete([]byte(request.ProtoSessionID)); err != nil {
			responses[i] = protosessionsv3.NewRemoveProtoSessionHitsResponse(err)
			continue
		}
		mapDeletions = append(mapDeletions, request.ProtoSessionID)
		responses[i] = protosessionsv3.NewRemoveProtoSessionHitsResponse(nil)
	}
	return mapDeletions
}

func (b *boltBatchedIOBackend) removeIdentifierMetadata(
	identifiersBucket *bolt.Bucket,
	requests []*protosessionsv3.RemoveAllHitRelatedMetadataRequest,
	responses []*protosessionsv3.RemoveAllHitRelatedMetadataResponse,
) {
	for i, request := range requests {
		key := b.identifierKey(request.IdentifierType, request.ExtractIdentifier, request.Hit)
		if err := identifiersBucket.Delete(key); err != nil {
			responses[i] = protosessionsv3.NewRemoveAllHitRelatedMetadataResponse(err)
			continue
		}
		responses[i] = protosessionsv3.NewRemoveAllHitRelatedMetadataResponse(nil)
	}
}

func (b *boltBatchedIOBackend) removeBucketMetadata(
	bucketsBucket *bolt.Bucket,
	requests []*protosessionsv3.RemoveBucketMetadataRequest,
	responses []*protosessionsv3.RemoveBucketMetadataResponse,
) {
	for i, request := range requests {
		key := b.bucketKey(request.BucketID)
		if bucketsBucket.Bucket(key) != nil {
			if err := bucketsBucket.DeleteBucket(key); err != nil {
				responses[i] = protosessionsv3.NewRemoveBucketMetadataResponse(err)
				continue
			}
		}
		responses[i] = protosessionsv3.NewRemoveBucketMetadataResponse(nil)
	}
}

// Cleanup removes hits, metadata, and bucket data in a single transaction
func (b *boltBatchedIOBackend) Cleanup(
	_ context.Context,
	hitsRequests []*protosessionsv3.RemoveProtoSessionHitsRequest,
	metadataRequests []*protosessionsv3.RemoveAllHitRelatedMetadataRequest,
	bucketMetadataRequests []*protosessionsv3.RemoveBucketMetadataRequest,
) (
	[]*protosessionsv3.RemoveProtoSessionHitsResponse,
	[]*protosessionsv3.RemoveAllHitRelatedMetadataResponse,
	[]*protosessionsv3.RemoveBucketMetadataResponse,
) {
	hitsResponses := make([]*protosessionsv3.RemoveProtoSessionHitsResponse, len(hitsRequests))
	metadataResponses := make([]*protosessionsv3.RemoveAllHitRelatedMetadataResponse, len(metadataRequests))
	bucketResponses := make([]*protosessionsv3.RemoveBucketMetadataResponse, len(bucketMetadataRequests))
	var mapDeletions []hits.ClientID

	err := b.db.Update(func(tx *bolt.Tx) error {
		sessionsBucket := tx.Bucket([]byte(protoSessionsBucket))
		identifiersBucket := tx.Bucket([]byte(identifiersBucket))
		bucketsBucket := tx.Bucket([]byte(bucketsBucket))
		mapBucket := tx.Bucket([]byte(sessionToBucketMapBucket))

		mapDeletions = b.removeProtoSessionHits(sessionsBucket, mapBucket, hitsRequests, hitsResponses, mapDeletions)
		b.removeIdentifierMetadata(identifiersBucket, metadataRequests, metadataResponses)
		b.removeBucketMetadata(bucketsBucket, bucketMetadataRequests, bucketResponses)

		return nil
	})

	if err != nil {
		fillNilHitsResponses(hitsResponses, err)
		fillNilMetadataResponses(metadataResponses, err)
		fillNilBucketResponses(bucketResponses, err)
	} else {
		// Apply map deletions after successful transaction
		b.sessionToBucketMu.Lock()
		for _, sessionID := range mapDeletions {
			delete(b.sessionToBucketMap, sessionID)
		}
		b.sessionToBucketMu.Unlock()
	}

	return hitsResponses, metadataResponses, bucketResponses
}

func fillNilHitsResponses(responses []*protosessionsv3.RemoveProtoSessionHitsResponse, err error) {
	for i := range responses {
		if responses[i] == nil {
			responses[i] = protosessionsv3.NewRemoveProtoSessionHitsResponse(err)
		}
	}
}

func fillNilMetadataResponses(responses []*protosessionsv3.RemoveAllHitRelatedMetadataResponse, err error) {
	for i := range responses {
		if responses[i] == nil {
			responses[i] = protosessionsv3.NewRemoveAllHitRelatedMetadataResponse(err)
		}
	}
}

func fillNilBucketResponses(responses []*protosessionsv3.RemoveBucketMetadataResponse, err error) {
	for i := range responses {
		if responses[i] == nil {
			responses[i] = protosessionsv3.NewRemoveBucketMetadataResponse(err)
		}
	}
}

// Stop closes the BoltDB connection
func (b *boltBatchedIOBackend) Stop(_ context.Context) error {
	return b.db.Close()
}
