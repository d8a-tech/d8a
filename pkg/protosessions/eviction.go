package protosessions

import "github.com/d8a-tech/d8a/pkg/hits"

// EvictionStrategy processes conflicting hits and determines eviction behavior
type EvictionStrategy func(
	hit *hits.Hit,
	conflict *IdentifierConflictResponse,
	hitsToBeSaved *[]*hits.Hit,
	protosessionsForEviction map[hits.ClientID][]*hits.Hit,
)

// RewriteIDAndUpdateInPlaceStrategy rewrites the hit's ID and updates it in place
func RewriteIDAndUpdateInPlaceStrategy(
	hit *hits.Hit,
	conflict *IdentifierConflictResponse,
	hitsToBeSaved *[]*hits.Hit,
	protosessionsForEviction map[hits.ClientID][]*hits.Hit,
) {
	MarkForEviction(hit, conflict.ConflictsWith)
	*hitsToBeSaved = append(*hitsToBeSaved, hit)
}

// EvictWholeProtosessionStrategy evicts entire protosession and re-queues it
func EvictWholeProtosessionStrategy(
	hit *hits.Hit,
	conflict *IdentifierConflictResponse,
	hitsToBeSaved *[]*hits.Hit,
	protosessionsForEviction map[hits.ClientID][]*hits.Hit,
) {
	MarkForEviction(hit, conflict.ConflictsWith)
	if protosessionsForEviction[conflict.ConflictsWith] == nil {
		protosessionsForEviction[conflict.ConflictsWith] = make([]*hits.Hit, 0)
	}
	protosessionsForEviction[conflict.ConflictsWith] = append(protosessionsForEviction[conflict.ConflictsWith], hit)
}
