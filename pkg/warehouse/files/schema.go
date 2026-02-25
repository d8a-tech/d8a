package files

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

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

// EscapeTableName replaces unsafe characters with underscores.
func EscapeTableName(table string) string {
	if table == "" {
		return table
	}

	var builder strings.Builder
	builder.Grow(len(table))
	for _, ch := range table {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			builder.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		case ch == '-' || ch == '_':
			builder.WriteRune(ch)
		default:
			builder.WriteRune('_')
		}
	}

	return builder.String()
}

// StreamDir returns the stream directory for a table+schema fingerprint.
func StreamDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(spoolDir, "streams", tableEsc, fingerprint)
}

// ActivePath returns the active segment path for a stream.
func ActivePath(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "active.csv")
}

// SealedDir returns the sealed segments directory for a stream.
func SealedDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "sealed")
}

// UploadingDir returns the uploading segments directory for a stream.
func UploadingDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "uploading")
}

// FailedDir returns the failed segments directory for a stream.
func FailedDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "failed")
}

// SegmentPath returns the path for a segment ID within a directory.
func SegmentPath(dir, segmentID string) string {
	return filepath.Join(dir, fmt.Sprintf("%s.csv", segmentID))
}

// FailCountPath returns the path for a segment fail counter within the stream directory.
func FailCountPath(streamDir, segmentID string) string {
	return filepath.Join(streamDir, fmt.Sprintf("%s.failcount", segmentID))
}

// SegmentRemoteKey returns the remote object key for a segment.
func SegmentRemoteKey(tableEsc, fingerprint, segmentID string, sealTime time.Time) string {
	date := sealTime.UTC().Format("2006/01/02")
	return fmt.Sprintf("table=%s/schema=%s/dt=%s/%s.csv", tableEsc, fingerprint, date, segmentID)
}

// Metadata represents schema information persisted to disk.
type Metadata struct {
	Table       string `json:"table"`
	Fingerprint string `json:"fingerprint"`
	Schema      string `json:"schema"`              // base64-encoded Arrow IPC schema
	CreatedAt   string `json:"created_at"`          // RFC3339 timestamp
	SealedAt    string `json:"sealed_at,omitempty"` // RFC3339 timestamp when sealed
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
	schemaBytes := flight.SerializeSchema(schema, memory.DefaultAllocator)

	encoded := base64.StdEncoding.EncodeToString(schemaBytes)
	return encoded, nil
}

// DeserializeSchema deserializes a base64-encoded Arrow schema string.
func DeserializeSchema(encoded string) (*arrow.Schema, error) {
	schemaBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("base64 decoding schema: %w", err)
	}

	schema, err := flight.DeserializeSchema(schemaBytes, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("deserializing schema: %w", err)
	}

	return schema, nil
}
