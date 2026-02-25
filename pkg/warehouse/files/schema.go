package files

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SchemaFingerprint returns a 16-character SHA256-based fingerprint for the schema.
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
//
// Multiple Write() calls for the same table+schema will append to the same file.
func FilenameForWrite(table, fingerprint string, format Format) string {
	ext := format.Extension()
	return fmt.Sprintf("%s_%s.%s", fingerprint, table, ext)
}

// MetadataFilename generates a metadata filename for a CSV file.
// Format: {fingerprint}_{table}.meta.json
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

// SerializeSchema serializes an Arrow schema to a base64-encoded string.
func SerializeSchema(schema *arrow.Schema) (string, error) {
	// Use flight.SerializeSchema to get bytes using Arrow IPC format
	schemaBytes := flight.SerializeSchema(schema, memory.DefaultAllocator)

	// Base64-encode the bytes
	encoded := base64.StdEncoding.EncodeToString(schemaBytes)
	return encoded, nil
}

// DeserializeSchema deserializes a base64-encoded Arrow schema string.
func DeserializeSchema(encoded string) (*arrow.Schema, error) {
	// Base64-decode the string
	schemaBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("base64 decoding schema: %w", err)
	}

	// Use flight.DeserializeSchema to parse the bytes
	schema, err := flight.DeserializeSchema(schemaBytes, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("deserializing schema: %w", err)
	}

	return schema, nil
}
