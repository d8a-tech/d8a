package files

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// SchemaFingerprint returns an 8-character fingerprint for the schema.
// Uses Arrow's built-in Fingerprint() method, truncated to 8 chars.
func SchemaFingerprint(schema *arrow.Schema) string {
	fp := schema.Fingerprint()
	if len(fp) > 8 {
		return fp[:8]
	}
	return fp
}

// FilenameForWrite generates a filename for writing data with the given parameters.
// Format: {fingerprint}_{table}_{timestamp}.{ext}
// Example: a3b5c7f9_events_2026-02-23T14-00-00Z.csv
//
// The timestamp is formatted in ISO 8601 with hyphens replacing colons
// for filesystem compatibility.
func FilenameForWrite(table, fingerprint string, timestamp time.Time, format Format) string {
	// Format timestamp as ISO 8601 with hyphens replacing colons
	// e.g., 2026-02-23T14-00-00Z instead of 2026-02-23T14:00:00Z
	// The format "2006-01-02T15-04-05" produces the desired timestamp format with hyphens
	timestampStr := timestamp.UTC().Format("2006-01-02T15-04-05Z")

	ext := format.Extension()
	return fmt.Sprintf("%s_%s_%s.%s", fingerprint, table, timestampStr, ext)
}
