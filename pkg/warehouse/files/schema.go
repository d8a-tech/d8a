package files

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

// SchemaFingerprint returns a 16-character SHA256-based fingerprint for the schema.
// Uses Arrow's built-in Fingerprint() method, computes SHA256 hash, and takes first 16 hex characters.
// Example: "a3b5c7f9e1d4b2a6"
func SchemaFingerprint(schema *arrow.Schema) string {
	arrowFp := schema.Fingerprint()
	hash := sha256.Sum256([]byte(arrowFp))
	hexStr := fmt.Sprintf("%x", hash)
	if len(hexStr) > 16 {
		return hexStr[:16]
	}
	return hexStr
}

// FilenameForWrite generates a filename for writing data with the given parameters.
// Format: {fingerprint}_{table}.{ext}
// Example: a3b5c7f9e1d4b2a6_events.csv
//
// Multiple Write() calls for the same table+schema will append to the same file.
func FilenameForWrite(table, fingerprint string, format Format) string {
	ext := format.Extension()
	return fmt.Sprintf("%s_%s.%s", fingerprint, table, ext)
}

// MetadataFilename generates a metadata filename for a CSV file.
// Format: {fingerprint}_{table}.meta.json
// Example: a3b5c7f9e1d4b2a6_events.meta.json
func MetadataFilename(table, fingerprint string) string {
	return fmt.Sprintf("%s_%s.meta.json", fingerprint, table)
}

// Metadata represents schema information persisted to disk.
// Stored as JSON in .meta.json files alongside CSV data.
type Metadata struct {
	Table       string `json:"table"`
	Fingerprint string `json:"fingerprint"`
	Schema      string `json:"schema"`     // base64-encoded Arrow IPC schema
	CreatedAt   string `json:"created_at"` // RFC3339 timestamp
}

// WriteMetadata writes metadata to the provided writer as JSON.
// The metadata describes the Arrow schema and table information for a CSV file.
func WriteMetadata(w io.Writer, metadata *Metadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("writing metadata: %w", err)
	}
	return nil
}

// ReadMetadata reads metadata from the provided reader.
// The metadata describes the Arrow schema and table information for a CSV file.
func ReadMetadata(r io.Reader) (*Metadata, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading metadata: %w", err)
	}
	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshaling metadata: %w", err)
	}
	return &metadata, nil
}
