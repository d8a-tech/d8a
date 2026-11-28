package bolt

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protosessions"
	bolt "go.etcd.io/bbolt"
)

const (
	annotationsBucket = "annotations"
	closeClaimsBucket = "closeClaims"
)

var _ protosessions.AnnotationBackend = (*boltBatchedIOBackend)(nil)

func (b *boltBatchedIOBackend) annotationsKey(clientID hits.ClientID) []byte {
	return []byte(fmt.Sprintf("sessions.annotations.%s", clientID))
}

func ensureAnnotationBuckets(tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(annotationsBucket)); err != nil {
		return err
	}
	_, err := tx.CreateBucketIfNotExists([]byte(closeClaimsBucket))
	return err
}

// LookupIdentifierOwners reads identifier owners without registering anything.
func (b *boltBatchedIOBackend) LookupIdentifierOwners(
	_ context.Context,
	requests []*protosessions.IdentifierConflictRequest,
) []*protosessions.LookupIdentifierOwnerResponse {
	responses := make([]*protosessions.LookupIdentifierOwnerResponse, len(requests))
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(identifiersBucket))
		for i, request := range requests {
			owner := bucket.Get(b.identifierKey(request.IdentifierType, request.ExtractIdentifier, request.Hit))
			if owner == nil {
				responses[i] = &protosessions.LookupIdentifierOwnerResponse{}
				continue
			}
			responses[i] = &protosessions.LookupIdentifierOwnerResponse{Owner: hits.ClientID(owner), Found: true}
		}
		return nil
	})
	if err != nil {
		for i := range responses {
			if responses[i] == nil {
				responses[i] = &protosessions.LookupIdentifierOwnerResponse{Err: err}
			}
		}
	}
	return responses
}

// AttachAnnotations stores annotations on open proto-sessions, in one transaction. The
// closing bucket is read from the persisted session-to-bucket map, the source of truth that
// marks and cleanups also write in their transactions.
func (b *boltBatchedIOBackend) AttachAnnotations(
	_ context.Context,
	requests []*protosessions.AttachAnnotationsRequest,
) []*protosessions.AttachAnnotationsResponse {
	responses := make([]*protosessions.AttachAnnotationsResponse, len(requests))
	encoded := make([][][]byte, len(requests))
	for i, request := range requests {
		payload, err := b.encodeHits(request.Annotations)
		if err != nil {
			responses[i] = &protosessions.AttachAnnotationsResponse{Err: err}
			continue
		}
		encoded[i] = payload
	}

	err := b.db.Update(func(tx *bolt.Tx) error {
		if err := ensureAnnotationBuckets(tx); err != nil {
			return err
		}
		mapBucket := tx.Bucket([]byte(sessionToBucketMapBucket))
		claims := tx.Bucket([]byte(closeClaimsBucket))
		annotations := tx.Bucket([]byte(annotationsBucket))
		for i, request := range requests {
			if responses[i] != nil {
				continue
			}
			attached, err := b.attach(mapBucket, claims, annotations, request, encoded[i])
			responses[i] = &protosessions.AttachAnnotationsResponse{Attached: attached, Err: err}
		}
		return nil
	})
	if err != nil {
		for i, request := range requests {
			if responses[i] == nil || responses[i].Err == nil {
				responses[i] = &protosessions.AttachAnnotationsResponse{
					Attached: make([]bool, len(request.Annotations)),
					Err:      err,
				}
			}
		}
	}
	return responses
}

func (b *boltBatchedIOBackend) attach(
	mapBucket, claims, annotations *bolt.Bucket,
	request *protosessions.AttachAnnotationsRequest,
	encoded [][]byte,
) ([]bool, error) {
	attached := make([]bool, len(request.Annotations))
	sessionID := []byte(request.ProtoSessionID)
	closing := mapBucket.Get(sessionID)
	if closing == nil || claims.Get(sessionID) != nil {
		return attached, nil
	}
	closingBucket := int64(binary.BigEndian.Uint64(closing)) //nolint:gosec // bucket IDs are always positive
	if closingBucket <= request.NowBucket {
		return attached, nil
	}
	stored, err := annotations.CreateBucketIfNotExists(b.annotationsKey(request.ProtoSessionID))
	if err != nil {
		return attached, err
	}
	count, size := 0, 0
	if err := stored.ForEach(func(k, _ []byte) error {
		count++
		size += len(k)
		return nil
	}); err != nil {
		return attached, err
	}
	for j, annotation := range encoded {
		if closingBucket <= request.EventBuckets[j] {
			continue
		}
		if stored.Get(annotation) != nil {
			attached[j] = true // the same annotation, e.g. from a retried task
			continue
		}
		if count >= protosessions.MaxAnnotationsPerProtoSession ||
			size+len(annotation) > protosessions.MaxAnnotationBytesPerProtoSession {
			continue
		}
		if err := stored.Put(annotation, []byte{1}); err != nil {
			return attached, err
		}
		count++
		size += len(annotation)
		attached[j] = true
	}
	return attached, nil
}

// ClaimForClose marks proto-sessions as being closed and returns their annotations.
func (b *boltBatchedIOBackend) ClaimForClose(
	_ context.Context,
	requests []*protosessions.ClaimForCloseRequest,
) []*protosessions.ClaimForCloseResponse {
	responses := make([]*protosessions.ClaimForCloseResponse, len(requests))
	encoded := make([][][]byte, len(requests))
	err := b.db.Update(func(tx *bolt.Tx) error {
		if err := ensureAnnotationBuckets(tx); err != nil {
			return err
		}
		claims := tx.Bucket([]byte(closeClaimsBucket))
		annotations := tx.Bucket([]byte(annotationsBucket))
		for i, request := range requests {
			if err := claims.Put([]byte(request.ProtoSessionID), []byte{1}); err != nil {
				responses[i] = &protosessions.ClaimForCloseResponse{Err: err}
				continue
			}
			stored := annotations.Bucket(b.annotationsKey(request.ProtoSessionID))
			if stored == nil {
				continue
			}
			payload, err := b.copyEncodedHitsFromBucket(stored)
			if err != nil {
				responses[i] = &protosessions.ClaimForCloseResponse{Err: err}
				continue
			}
			encoded[i] = payload
		}
		return nil
	})
	for i := range requests {
		if responses[i] != nil {
			continue
		}
		if err != nil {
			responses[i] = &protosessions.ClaimForCloseResponse{Err: err}
			continue
		}
		decoded, decErr := b.decodeHits(encoded[i])
		responses[i] = &protosessions.ClaimForCloseResponse{Annotations: decoded, Err: decErr}
	}
	return responses
}

// removeAnnotationState deletes a proto-session's annotations and close claim, with its hits.
func (b *boltBatchedIOBackend) removeAnnotationState(tx *bolt.Tx, sessionID hits.ClientID) error {
	if err := ensureAnnotationBuckets(tx); err != nil {
		return err
	}
	annotations := tx.Bucket([]byte(annotationsBucket))
	key := b.annotationsKey(sessionID)
	if annotations.Bucket(key) != nil {
		if err := annotations.DeleteBucket(key); err != nil {
			return err
		}
	}
	return tx.Bucket([]byte(closeClaimsBucket)).Delete([]byte(sessionID))
}
