package files

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

// TestSchemaFingerprint_Returns16CharHash verifies SHA256-based fingerprint generation.
func TestSchemaFingerprint_Returns16CharHash(t *testing.T) {
	// given: a simple schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// when: fingerprinting the schema
	fp := schemaFingerprint(schema)

	// then: result is exactly 16 hex characters (lowercase)
	assert.Equal(t, 16, len(fp))
	for _, c := range fp {
		assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
			"fingerprint should be lowercase hex: %c", c)
	}
}

// TestSchemaFingerprint_ConsistentForSameSchema verifies deterministic hashing.
func TestSchemaFingerprint_ConsistentForSameSchema(t *testing.T) {
	// given: the same schema hashed twice
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	// when: fingerprinting twice
	fp1 := schemaFingerprint(schema)
	fp2 := schemaFingerprint(schema)

	// then: fingerprints are identical
	assert.Equal(t, fp1, fp2)
}

// TestSchemaFingerprint_DifferentForDifferentSchemas verifies fingerprints differ for different schemas.
func TestSchemaFingerprint_DifferentForDifferentSchemas(t *testing.T) {
	// given: two different schemas
	schema1 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	schema2 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32}, // Different type
		},
		nil,
	)

	// when: fingerprinting both
	fp1 := schemaFingerprint(schema1)
	fp2 := schemaFingerprint(schema2)

	// then: fingerprints differ
	assert.NotEqual(t, fp1, fp2)
}

func TestEscapeTableName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "safe characters",
			input:    "events_2026-02-25",
			expected: "events_2026-02-25",
		},
		{
			name:     "unsafe characters",
			input:    "events$#%",
			expected: "events___",
		},
		{
			name:     "path traversal",
			input:    "../../etc/passwd",
			expected: "______etc_passwd",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, escapeTableName(tt.input))
		})
	}
}

func TestStreamPaths(t *testing.T) {
	spoolDir := "/spool"
	tableEsc := "events"
	fingerprint := "abc123"
	segmentID := "seg-1"

	assert.Equal(t, activePath(spoolDir, tableEsc, fingerprint, "csv"), activePath(spoolDir, tableEsc, fingerprint, "csv"))
	assert.Equal(t, sealedDir(spoolDir, tableEsc, fingerprint), sealedDir(spoolDir, tableEsc, fingerprint))
	assert.Equal(t, uploadingDir(spoolDir, tableEsc, fingerprint), uploadingDir(spoolDir, tableEsc, fingerprint))
	assert.Equal(t, failedDir(spoolDir, tableEsc, fingerprint), failedDir(spoolDir, tableEsc, fingerprint))

	streamDir := streamDir(spoolDir, tableEsc, fingerprint)
	assert.Equal(t, filepath.Join(spoolDir, "streams", tableEsc, fingerprint), streamDir)

	assert.Equal(t, filepath.Join(streamDir, "active.csv"), activePath(spoolDir, tableEsc, fingerprint, "csv"))
	assert.Equal(t, filepath.Join(streamDir, "sealed"), sealedDir(spoolDir, tableEsc, fingerprint))
	assert.Equal(t, filepath.Join(streamDir, "uploading"), uploadingDir(spoolDir, tableEsc, fingerprint))
	assert.Equal(t, filepath.Join(streamDir, "failed"), failedDir(spoolDir, tableEsc, fingerprint))

	sealedPath := segmentPath(sealedDir(spoolDir, tableEsc, fingerprint), segmentID, "csv")
	assert.Equal(t, filepath.Join(streamDir, "sealed", "seg-1.csv"), sealedPath)

	failCountPath := failCountPath(streamDir, segmentID)
	assert.Equal(t, filepath.Join(streamDir, "seg-1.failcount"), failCountPath)
}

func TestSegmentRemoteKey(t *testing.T) {
	sealTime := time.Date(2026, time.February, 25, 10, 2, 3, 0, time.UTC)
	key := segmentRemoteKey("events", "abc123", "seg-1", "csv", sealTime)
	assert.Equal(t, "table=events/schema=abc123/dt=2026/02/25/seg-1.csv", key)
}
