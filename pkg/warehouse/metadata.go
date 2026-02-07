package warehouse

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// GetArrowMetadataValue retrieves a value from Arrow metadata by key.
// Returns the value and true if found, empty string and false otherwise.
func GetArrowMetadataValue(md arrow.Metadata, key string) (string, bool) {
	if md.Len() == 0 {
		return "", false
	}
	keys := md.Keys()
	values := md.Values()
	for i, k := range keys {
		if k == key {
			return values[i], true
		}
	}
	return "", false
}

// MergeArrowMetadata creates a new Arrow metadata by merging an existing metadata
// with a new key-value pair. If the key already exists, it will be overwritten.
func MergeArrowMetadata(existing arrow.Metadata, key, value string) arrow.Metadata {
	if existing.Len() == 0 {
		return arrow.NewMetadata([]string{key}, []string{value})
	}

	keys := existing.Keys()
	values := existing.Values()

	// Check if key already exists
	found := false
	for i, k := range keys {
		if k == key {
			values[i] = value
			found = true
			break
		}
	}

	if found {
		return arrow.NewMetadata(keys, values)
	}

	// Append new key-value pair
	newKeys := make([]string, len(keys)+1)
	newValues := make([]string, len(values)+1)
	copy(newKeys, keys)
	copy(newValues, values)
	newKeys[len(keys)] = key
	newValues[len(values)] = value

	return arrow.NewMetadata(newKeys, newValues)
}
