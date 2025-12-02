package protosessionsv3

import (
	"context"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/hits"
)

type deduplicatingBatchedIOBackend struct {
	underlying BatchedIOBackend
}

// NewDeduplicatingBatchedIOBackend wraps a BatchedIOBackend and deduplicates requests
func NewDeduplicatingBatchedIOBackend(underlying BatchedIOBackend) BatchedIOBackend {
	return &deduplicatingBatchedIOBackend{underlying: underlying}
}

func (d *deduplicatingBatchedIOBackend) GetIdentifierConflicts(
	ctx context.Context,
	requests []*IdentifierConflictRequest,
) []*IdentifierConflictResponse {
	if len(requests) == 0 {
		return nil
	}

	// Key: "identifierType:identifierValue"
	seen := make(map[string]int) // key -> index in deduplicated slice
	var deduplicated []*IdentifierConflictRequest
	originalToDedup := make([]int, len(requests))

	for i, req := range requests {
		key := fmt.Sprintf("%s:%s", req.IdentifierType, req.ExtractIdentifier(req.Hit))
		if idx, exists := seen[key]; exists {
			originalToDedup[i] = idx
		} else {
			idx := len(deduplicated)
			seen[key] = idx
			originalToDedup[i] = idx
			deduplicated = append(deduplicated, req)
		}
	}

	dedupResponses := d.underlying.GetIdentifierConflicts(ctx, deduplicated)

	responses := make([]*IdentifierConflictResponse, len(requests))
	for i := range requests {
		responses[i] = dedupResponses[originalToDedup[i]]
	}
	return responses
}

type markKey struct {
	ProtoSessionID hits.ClientID
	BucketID       int64
}

func deduplicateAppendRequests(
	requests []*AppendHitsToProtoSessionRequest,
) (dedup []*AppendHitsToProtoSessionRequest, originalToDedup []int) {
	seen := make(map[hits.ClientID]int)
	originalToDedup = make([]int, len(requests))

	for i, req := range requests {
		if idx, exists := seen[req.ProtoSessionID]; exists {
			originalToDedup[i] = idx
			dedup[idx].Hits = append(dedup[idx].Hits, req.Hits...)
		} else {
			idx := len(dedup)
			seen[req.ProtoSessionID] = idx
			originalToDedup[i] = idx
			hitsCopy := make([]*hits.Hit, len(req.Hits))
			copy(hitsCopy, req.Hits)
			dedup = append(dedup, &AppendHitsToProtoSessionRequest{
				ProtoSessionID: req.ProtoSessionID,
				Hits:           hitsCopy,
			})
		}
	}
	return dedup, originalToDedup
}

func deduplicateGetRequests(
	requests []*GetProtoSessionHitsRequest,
) (dedup []*GetProtoSessionHitsRequest, originalToDedup []int) {
	seen := make(map[hits.ClientID]int)
	originalToDedup = make([]int, len(requests))

	for i, req := range requests {
		if idx, exists := seen[req.ProtoSessionID]; exists {
			originalToDedup[i] = idx
		} else {
			idx := len(dedup)
			seen[req.ProtoSessionID] = idx
			originalToDedup[i] = idx
			dedup = append(dedup, req)
		}
	}
	return dedup, originalToDedup
}

func deduplicateMarkRequests(
	requests []*MarkProtoSessionClosingForGivenBucketRequest,
) (dedup []*MarkProtoSessionClosingForGivenBucketRequest, originalToDedup []int) {
	seen := make(map[markKey]int)
	originalToDedup = make([]int, len(requests))

	for i, req := range requests {
		key := markKey{ProtoSessionID: req.ProtoSessionID, BucketID: req.BucketID}
		if idx, exists := seen[key]; exists {
			originalToDedup[i] = idx
		} else {
			idx := len(dedup)
			seen[key] = idx
			originalToDedup[i] = idx
			dedup = append(dedup, req)
		}
	}
	return dedup, originalToDedup
}

func (d *deduplicatingBatchedIOBackend) HandleBatch(
	ctx context.Context,
	appendHitsRequests []*AppendHitsToProtoSessionRequest,
	getProtoSessionHitsRequests []*GetProtoSessionHitsRequest,
	markProtoSessionClosingForGivenBucketRequests []*MarkProtoSessionClosingForGivenBucketRequest,
) (
	[]*AppendHitsToProtoSessionResponse,
	[]*GetProtoSessionHitsResponse,
	[]*MarkProtoSessionClosingForGivenBucketResponse,
) {
	appendDedup, appendOriginalToDedup := deduplicateAppendRequests(appendHitsRequests)
	getDedup, getOriginalToDedup := deduplicateGetRequests(getProtoSessionHitsRequests)
	markDedup, markOriginalToDedup := deduplicateMarkRequests(markProtoSessionClosingForGivenBucketRequests)

	appendDedupResp, getDedupResp, markDedupResp := d.underlying.HandleBatch(
		ctx, appendDedup, getDedup, markDedup,
	)

	appendResponses := make([]*AppendHitsToProtoSessionResponse, len(appendHitsRequests))
	for i := range appendHitsRequests {
		appendResponses[i] = appendDedupResp[appendOriginalToDedup[i]]
	}

	getResponses := make([]*GetProtoSessionHitsResponse, len(getProtoSessionHitsRequests))
	for i := range getProtoSessionHitsRequests {
		getResponses[i] = getDedupResp[getOriginalToDedup[i]]
	}

	markReqs := markProtoSessionClosingForGivenBucketRequests
	markResponses := make([]*MarkProtoSessionClosingForGivenBucketResponse, len(markReqs))
	for i := range markReqs {
		markResponses[i] = markDedupResp[markOriginalToDedup[i]]
	}

	return appendResponses, getResponses, markResponses
}

func (d *deduplicatingBatchedIOBackend) GetAllProtosessionsForBucket(
	ctx context.Context,
	requests []*GetAllProtosessionsForBucketRequest,
) []*GetAllProtosessionsForBucketResponse {
	if len(requests) == 0 {
		return nil
	}

	seen := make(map[int64]int)
	var deduplicated []*GetAllProtosessionsForBucketRequest
	originalToDedup := make([]int, len(requests))

	for i, req := range requests {
		if idx, exists := seen[req.BucketID]; exists {
			originalToDedup[i] = idx
		} else {
			idx := len(deduplicated)
			seen[req.BucketID] = idx
			originalToDedup[i] = idx
			deduplicated = append(deduplicated, req)
		}
	}

	dedupResponses := d.underlying.GetAllProtosessionsForBucket(ctx, deduplicated)

	responses := make([]*GetAllProtosessionsForBucketResponse, len(requests))
	for i := range requests {
		responses[i] = dedupResponses[originalToDedup[i]]
	}
	return responses
}

func deduplicateHitsRequests(
	requests []*RemoveProtoSessionHitsRequest,
) (dedup []*RemoveProtoSessionHitsRequest, originalToDedup []int) {
	seen := make(map[hits.ClientID]int)
	originalToDedup = make([]int, len(requests))

	for i, req := range requests {
		if idx, exists := seen[req.ProtoSessionID]; exists {
			originalToDedup[i] = idx
		} else {
			idx := len(dedup)
			seen[req.ProtoSessionID] = idx
			originalToDedup[i] = idx
			dedup = append(dedup, req)
		}
	}
	return dedup, originalToDedup
}

func deduplicateMetadataRequests(
	requests []*RemoveAllHitRelatedMetadataRequest,
) (dedup []*RemoveAllHitRelatedMetadataRequest, originalToDedup []int) {
	seen := make(map[string]int)
	originalToDedup = make([]int, len(requests))

	for i, req := range requests {
		key := fmt.Sprintf("%s:%s", req.IdentifierType, req.ExtractIdentifier(req.Hit))
		if idx, exists := seen[key]; exists {
			originalToDedup[i] = idx
		} else {
			idx := len(dedup)
			seen[key] = idx
			originalToDedup[i] = idx
			dedup = append(dedup, req)
		}
	}
	return dedup, originalToDedup
}

func deduplicateBucketRequests(
	requests []*RemoveBucketMetadataRequest,
) (dedup []*RemoveBucketMetadataRequest, originalToDedup []int) {
	seen := make(map[int64]int)
	originalToDedup = make([]int, len(requests))

	for i, req := range requests {
		if idx, exists := seen[req.BucketID]; exists {
			originalToDedup[i] = idx
		} else {
			idx := len(dedup)
			seen[req.BucketID] = idx
			originalToDedup[i] = idx
			dedup = append(dedup, req)
		}
	}
	return dedup, originalToDedup
}

func (d *deduplicatingBatchedIOBackend) Cleanup(
	ctx context.Context,
	hitsRequests []*RemoveProtoSessionHitsRequest,
	metadataRequests []*RemoveAllHitRelatedMetadataRequest,
	bucketMetadataRequests []*RemoveBucketMetadataRequest,
) (
	[]*RemoveProtoSessionHitsResponse,
	[]*RemoveAllHitRelatedMetadataResponse,
	[]*RemoveBucketMetadataResponse,
) {
	hitsDedup, hitsOriginalToDedup := deduplicateHitsRequests(hitsRequests)
	metadataDedup, metadataOriginalToDedup := deduplicateMetadataRequests(metadataRequests)
	bucketDedup, bucketOriginalToDedup := deduplicateBucketRequests(bucketMetadataRequests)

	hitsDedupResp, metadataDedupResp, bucketDedupResp := d.underlying.Cleanup(
		ctx, hitsDedup, metadataDedup, bucketDedup,
	)

	hitsResponses := make([]*RemoveProtoSessionHitsResponse, len(hitsRequests))
	for i := range hitsRequests {
		hitsResponses[i] = hitsDedupResp[hitsOriginalToDedup[i]]
	}

	metadataResponses := make([]*RemoveAllHitRelatedMetadataResponse, len(metadataRequests))
	for i := range metadataRequests {
		metadataResponses[i] = metadataDedupResp[metadataOriginalToDedup[i]]
	}

	bucketResponses := make([]*RemoveBucketMetadataResponse, len(bucketMetadataRequests))
	for i := range bucketMetadataRequests {
		bucketResponses[i] = bucketDedupResp[bucketOriginalToDedup[i]]
	}

	return hitsResponses, metadataResponses, bucketResponses
}

func (d *deduplicatingBatchedIOBackend) Stop(ctx context.Context) error {
	return d.underlying.Stop(ctx)
}
