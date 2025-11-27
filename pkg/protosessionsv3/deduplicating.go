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
	// Deduplicate append requests (merge hits for same proto-session)
	appendSeen := make(map[hits.ClientID]int)
	var appendDedup []*AppendHitsToProtoSessionRequest
	appendOriginalToDedup := make([]int, len(appendHitsRequests))

	for i, req := range appendHitsRequests {
		if idx, exists := appendSeen[req.ProtoSessionID]; exists {
			appendOriginalToDedup[i] = idx
			appendDedup[idx].Hits = append(appendDedup[idx].Hits, req.Hits...)
		} else {
			idx := len(appendDedup)
			appendSeen[req.ProtoSessionID] = idx
			appendOriginalToDedup[i] = idx
			// Copy hits to avoid mutating original
			hitsCopy := make([]*hits.Hit, len(req.Hits))
			copy(hitsCopy, req.Hits)
			appendDedup = append(appendDedup, &AppendHitsToProtoSessionRequest{
				ProtoSessionID: req.ProtoSessionID,
				Hits:           hitsCopy,
			})
		}
	}

	// Deduplicate get requests
	getSeen := make(map[hits.ClientID]int)
	var getDedup []*GetProtoSessionHitsRequest
	getOriginalToDedup := make([]int, len(getProtoSessionHitsRequests))

	for i, req := range getProtoSessionHitsRequests {
		if idx, exists := getSeen[req.ProtoSessionID]; exists {
			getOriginalToDedup[i] = idx
		} else {
			idx := len(getDedup)
			getSeen[req.ProtoSessionID] = idx
			getOriginalToDedup[i] = idx
			getDedup = append(getDedup, req)
		}
	}

	// Deduplicate mark requests
	type markKey struct {
		ProtoSessionID hits.ClientID
		BucketID       int64
	}
	markSeen := make(map[markKey]int)
	var markDedup []*MarkProtoSessionClosingForGivenBucketRequest
	markOriginalToDedup := make([]int, len(markProtoSessionClosingForGivenBucketRequests))

	for i, req := range markProtoSessionClosingForGivenBucketRequests {
		key := markKey{ProtoSessionID: req.ProtoSessionID, BucketID: req.BucketID}
		if idx, exists := markSeen[key]; exists {
			markOriginalToDedup[i] = idx
		} else {
			idx := len(markDedup)
			markSeen[key] = idx
			markOriginalToDedup[i] = idx
			markDedup = append(markDedup, req)
		}
	}

	appendDedupResp, getDedupResp, markDedupResp := d.underlying.HandleBatch(
		ctx, appendDedup, getDedup, markDedup,
	)

	// Map back
	appendResponses := make([]*AppendHitsToProtoSessionResponse, len(appendHitsRequests))
	for i := range appendHitsRequests {
		appendResponses[i] = appendDedupResp[appendOriginalToDedup[i]]
	}

	getResponses := make([]*GetProtoSessionHitsResponse, len(getProtoSessionHitsRequests))
	for i := range getProtoSessionHitsRequests {
		getResponses[i] = getDedupResp[getOriginalToDedup[i]]
	}

	markResponses := make([]*MarkProtoSessionClosingForGivenBucketResponse, len(markProtoSessionClosingForGivenBucketRequests))
	for i := range markProtoSessionClosingForGivenBucketRequests {
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
	// Deduplicate hits removal
	hitsSeen := make(map[hits.ClientID]int)
	var hitsDedup []*RemoveProtoSessionHitsRequest
	hitsOriginalToDedup := make([]int, len(hitsRequests))

	for i, req := range hitsRequests {
		if idx, exists := hitsSeen[req.ProtoSessionID]; exists {
			hitsOriginalToDedup[i] = idx
		} else {
			idx := len(hitsDedup)
			hitsSeen[req.ProtoSessionID] = idx
			hitsOriginalToDedup[i] = idx
			hitsDedup = append(hitsDedup, req)
		}
	}

	// Deduplicate metadata removal
	metadataSeen := make(map[string]int)
	var metadataDedup []*RemoveAllHitRelatedMetadataRequest
	metadataOriginalToDedup := make([]int, len(metadataRequests))

	for i, req := range metadataRequests {
		key := fmt.Sprintf("%s:%s", req.IdentifierType, req.ExtractIdentifier(req.Hit))
		if idx, exists := metadataSeen[key]; exists {
			metadataOriginalToDedup[i] = idx
		} else {
			idx := len(metadataDedup)
			metadataSeen[key] = idx
			metadataOriginalToDedup[i] = idx
			metadataDedup = append(metadataDedup, req)
		}
	}

	// Deduplicate bucket metadata removal
	bucketSeen := make(map[int64]int)
	var bucketDedup []*RemoveBucketMetadataRequest
	bucketOriginalToDedup := make([]int, len(bucketMetadataRequests))

	for i, req := range bucketMetadataRequests {
		if idx, exists := bucketSeen[req.BucketID]; exists {
			bucketOriginalToDedup[i] = idx
		} else {
			idx := len(bucketDedup)
			bucketSeen[req.BucketID] = idx
			bucketOriginalToDedup[i] = idx
			bucketDedup = append(bucketDedup, req)
		}
	}

	hitsDedupResp, metadataDedupResp, bucketDedupResp := d.underlying.Cleanup(
		ctx, hitsDedup, metadataDedup, bucketDedup,
	)

	// Map back
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
