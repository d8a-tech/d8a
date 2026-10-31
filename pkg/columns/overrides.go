package columns

import "github.com/d8a-tech/d8a/pkg/schema"

const (
	overrideMetadataPrefix = "transformations.override."
	unsetMetadataPrefix    = "transformations.unset."
)

// OverrideMetadataKey is the hit metadata key that overrides a derived event column value.
func OverrideMetadataKey(field string) string { return overrideMetadataPrefix + field }

// UnsetMetadataKey is the hit metadata key that clears a derived event column value.
func UnsetMetadataKey(field string) string { return unsetMetadataPrefix + field }

// ReadOverride returns a value written for a derived column by early transformations.
// Overrides travel with the hit, so they apply wherever the column is computed.
func ReadOverride(event *schema.Event, field string) (value string, set, unset bool) {
	if event == nil || event.BoundHit == nil || event.BoundHit.Metadata == nil {
		return "", false, false
	}
	if event.BoundHit.Metadata[UnsetMetadataKey(field)] != "" {
		return "", false, true
	}
	value, set = event.BoundHit.Metadata[OverrideMetadataKey(field)]
	return value, set, false
}
