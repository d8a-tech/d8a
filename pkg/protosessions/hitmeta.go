package protosessions

import "github.com/d8a-tech/d8a/pkg/hits"

const (
	MetaOriginalAuthoritativeClientIDKey = "original_authoritative_client_id"
	MetaMarkedForEvictionKey             = "marked_for_eviction"
)

func MarkForEviction(hit *hits.Hit, targetClientID hits.ClientID) {
	hit.Metadata[MetaOriginalAuthoritativeClientIDKey] = string(hit.AuthoritativeClientID)
	hit.Metadata[MetaMarkedForEvictionKey] = "true"
	hit.AuthoritativeClientID = targetClientID
}

func IsMarkedForEviction(hit *hits.Hit) bool {
	return hit.Metadata[MetaMarkedForEvictionKey] == "true"
}

func GetOriginalAuthoritativeClientID(hit *hits.Hit) (hits.ClientID, bool) {
	value, ok := hit.Metadata[MetaOriginalAuthoritativeClientIDKey]
	if !ok {
		return "", false
	}
	return hits.ClientID(value), true
}
