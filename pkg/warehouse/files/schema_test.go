package files

import (
	"bytes"
	"testing"

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
	fp := SchemaFingerprint(schema)

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
	fp1 := SchemaFingerprint(schema)
	fp2 := SchemaFingerprint(schema)

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
	fp1 := SchemaFingerprint(schema1)
	fp2 := SchemaFingerprint(schema2)

	// then: fingerprints differ
	assert.NotEqual(t, fp1, fp2)
}

// TestFilenameForWrite_GeneratesCorrectFilename verifies filename format.
func TestFilenameForWrite_GeneratesCorrectFilename(t *testing.T) {
	tests := []struct {
		name             string
		table            string
		fingerprint      string
		format           Format
		expectedFilename string
	}{
		{
			name:             "csv format",
			table:            "events",
			fingerprint:      "a3b5c7f9e1d4b2a6",
			format:           NewCSVFormat(),
			expectedFilename: "a3b5c7f9e1d4b2a6_events.csv",
		},
		{
			name:             "table with underscores",
			table:            "user_events",
			fingerprint:      "1234567890abcdef",
			format:           NewCSVFormat(),
			expectedFilename: "1234567890abcdef_user_events.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when: generating filename
			filename := FilenameForWrite(tt.table, tt.fingerprint, tt.format)

			// then: filename matches expected format
			assert.Equal(t, tt.expectedFilename, filename)
		})
	}
}

// TestFilenameForWrite_NoTimestamp verifies no timestamp in filename.
func TestFilenameForWrite_NoTimestamp(t *testing.T) {
	// given
	table := "events"
	fingerprint := "abc123def456"
	format := NewCSVFormat()

	// when: generating filename twice
	filename1 := FilenameForWrite(table, fingerprint, format)
	filename2 := FilenameForWrite(table, fingerprint, format)

	// then: filenames are identical (no timestamp)
	assert.Equal(t, filename1, filename2)
	assert.NotContains(t, filename1, "Z") // No Z suffix from timestamps
}

// TestMetadataFilename_GeneratesCorrectPath verifies metadata filename format.
func TestMetadataFilename_GeneratesCorrectPath(t *testing.T) {
	// given
	table := "events"
	fingerprint := "a3b5c7f9e1d4b2a6"

	// when: generating metadata filename
	metaFilename := MetadataFilename(table, fingerprint)

	// then: format is {fingerprint}_{table}.meta.json
	assert.Equal(t, "a3b5c7f9e1d4b2a6_events.meta.json", metaFilename)
}

// TestMetadataStruct_CorrectJSONTags verifies Metadata struct JSON marshaling.
func TestMetadataStruct_CorrectJSONTags(t *testing.T) {
	// given: a metadata instance
	metadata := &Metadata{
		Table:       "events",
		Fingerprint: "a3b5c7f9e1d4b2a6",
		Schema:      "base64-encoded-schema",
		CreatedAt:   "2026-02-24T14:30:00Z",
	}

	// when: writing metadata
	var buf bytes.Buffer
	err := WriteMetadata(&buf, metadata)
	assert.NoError(t, err)

	// then: JSON contains expected keys
	jsonStr := buf.String()
	assert.Contains(t, jsonStr, "\"table\"")
	assert.Contains(t, jsonStr, "\"fingerprint\"")
	assert.Contains(t, jsonStr, "\"schema\"")
	assert.Contains(t, jsonStr, "\"created_at\"")
	assert.Contains(t, jsonStr, "events")
	assert.Contains(t, jsonStr, "a3b5c7f9e1d4b2a6")
}

// TestWriteMetadata_SerializesCorrectly verifies metadata serialization.
func TestWriteMetadata_SerializesCorrectly(t *testing.T) {
	// given: a metadata instance
	metadata := &Metadata{
		Table:       "events",
		Fingerprint: "a3b5c7f9e1d4b2a6",
		Schema:      "base64-encoded-schema",
		CreatedAt:   "2026-02-24T14:30:00Z",
	}

	// when: writing metadata to buffer
	var buf bytes.Buffer
	err := WriteMetadata(&buf, metadata)

	// then: no error and buffer has content
	assert.NoError(t, err)
	assert.Greater(t, buf.Len(), 0)
}

// TestReadMetadata_DeserializesCorrectly verifies metadata deserialization.
func TestReadMetadata_DeserializesCorrectly(t *testing.T) {
	// given: metadata written to buffer
	original := &Metadata{
		Table:       "events",
		Fingerprint: "a3b5c7f9e1d4b2a6",
		Schema:      "base64-encoded-schema",
		CreatedAt:   "2026-02-24T14:30:00Z",
	}

	var buf bytes.Buffer
	err := WriteMetadata(&buf, original)
	assert.NoError(t, err)

	// when: reading metadata back from buffer
	reader := bytes.NewReader(buf.Bytes())
	read, err := ReadMetadata(reader)

	// then: deserialized metadata matches original
	assert.NoError(t, err)
	assert.Equal(t, original.Table, read.Table)
	assert.Equal(t, original.Fingerprint, read.Fingerprint)
	assert.Equal(t, original.Schema, read.Schema)
	assert.Equal(t, original.CreatedAt, read.CreatedAt)
}

// TestReadMetadata_InvalidJSON returns error for malformed JSON.
func TestReadMetadata_InvalidJSON(t *testing.T) {
	// given: invalid JSON data
	invalidJSON := bytes.NewBufferString("not valid json")

	// when: reading metadata
	_, err := ReadMetadata(invalidJSON)

	// then: error is returned
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshaling metadata")
}

// TestMetadata_RoundTripSerialization verifies write-read cycle preserves data.
func TestMetadata_RoundTripSerialization(t *testing.T) {
	tests := []struct {
		name     string
		metadata *Metadata
	}{
		{
			name: "simple metadata",
			metadata: &Metadata{
				Table:       "events",
				Fingerprint: "a3b5c7f9e1d4b2a6",
				Schema:      "base64-schema",
				CreatedAt:   "2026-02-24T14:30:00Z",
			},
		},
		{
			name: "metadata with special characters",
			metadata: &Metadata{
				Table:       "user_events",
				Fingerprint: "1234567890abcdef",
				Schema:      "VGhpcyBpcyBhIHRlc3Q=", // base64 encoded
				CreatedAt:   "2026-02-24T14:30:00.123Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given: metadata
			original := tt.metadata

			// when: round-trip through serialization
			var buf bytes.Buffer
			err := WriteMetadata(&buf, original)
			assert.NoError(t, err)

			reader := bytes.NewReader(buf.Bytes())
			read, err := ReadMetadata(reader)
			assert.NoError(t, err)

			// then: all fields preserved
			assert.Equal(t, original.Table, read.Table)
			assert.Equal(t, original.Fingerprint, read.Fingerprint)
			assert.Equal(t, original.Schema, read.Schema)
			assert.Equal(t, original.CreatedAt, read.CreatedAt)
		})
	}
}

// TestSchemaFingerprint_Deterministic verifies fingerprint is deterministic.
func TestSchemaFingerprint_Deterministic(t *testing.T) {
	// given: multiple schema instances with same definition
	schema1 := createTestSchema()
	schema2 := createTestSchema()

	// when: fingerprinting both
	fp1 := SchemaFingerprint(schema1)
	fp2 := SchemaFingerprint(schema2)

	// then: fingerprints are identical despite being different instances
	assert.Equal(t, fp1, fp2)
}

// Helper function to create a consistent test schema.
func createTestSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
		},
		nil,
	)
}

// TestSerializeSchema_Success verifies schema serialization produces base64 string.
func TestSerializeSchema_Success(t *testing.T) {
	// given: a schema
	schema := createTestSchema()

	// when: serializing the schema
	encoded, err := SerializeSchema(schema)

	// then: no error and result is non-empty base64 string
	assert.NoError(t, err)
	assert.NotEmpty(t, encoded)
	// Base64 strings should only contain alphanumeric, +, /, and =
	for _, c := range encoded {
		assert.True(t,
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=',
			"invalid base64 character: %c", c)
	}
}

// TestDeserializeSchema_Success verifies schema deserialization works correctly.
func TestDeserializeSchema_Success(t *testing.T) {
	// given: an encoded schema
	original := createTestSchema()
	encoded, err := SerializeSchema(original)
	assert.NoError(t, err)

	// when: deserializing the schema
	deserialized, err := DeserializeSchema(encoded)

	// then: no error and schema matches original
	assert.NoError(t, err)
	assert.NotNil(t, deserialized)
	assert.Equal(t, len(original.Fields()), len(deserialized.Fields()))
	for i, field := range original.Fields() {
		assert.Equal(t, field.Name, deserialized.Fields()[i].Name)
		assert.Equal(t, field.Type, deserialized.Fields()[i].Type)
	}
}

// TestSerializeDeserializeSchema_RoundTrip verifies schema serialization round-trip.
func TestSerializeDeserializeSchema_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		schema *arrow.Schema
	}{
		{
			name: "simple schema",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				},
				nil,
			),
		},
		{
			name: "complex schema with multiple types",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "name", Type: arrow.BinaryTypes.String},
					{Name: "active", Type: arrow.BinaryTypes.String},
					{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
					{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
				},
				nil,
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when: round-trip serialize and deserialize
			encoded, err := SerializeSchema(tt.schema)
			assert.NoError(t, err)

			deserialized, err := DeserializeSchema(encoded)
			assert.NoError(t, err)

			// then: schema is equivalent
			assert.Equal(t, len(tt.schema.Fields()), len(deserialized.Fields()))
			for i, field := range tt.schema.Fields() {
				assert.Equal(t, field.Name, deserialized.Fields()[i].Name)
				assert.Equal(t, field.Type, deserialized.Fields()[i].Type)
			}
		})
	}
}

// TestDeserializeSchema_InvalidBase64 returns error for invalid input.
func TestDeserializeSchema_InvalidBase64(t *testing.T) {
	// given: invalid base64 string
	invalidBase64 := "!!!not valid base64!!!"

	// when: deserializing
	_, err := DeserializeSchema(invalidBase64)

	// then: error is returned
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "base64 decoding schema")
}

// TestDeserializeSchema_InvalidData returns error for malformed schema data.
func TestDeserializeSchema_InvalidData(t *testing.T) {
	// given: valid base64 but invalid schema data
	invalidData := "aW52YWxpZCBkYXRh" // base64 of "invalid data"

	// when: deserializing
	_, err := DeserializeSchema(invalidData)

	// then: error is returned
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deserializing schema")
}

// TestSerializeDeserializeWithMetadataFile verifies schema serialization works with metadata file operations.
func TestSerializeDeserializeWithMetadataFile(t *testing.T) {
	// given: a test schema and temporary directory
	originalSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "email", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	spoolDir := t.TempDir()

	// when: serializing schema and saving metadata file
	serializedSchema, err := SerializeSchema(originalSchema)
	assert.NoError(t, err)

	fingerprint := SchemaFingerprint(originalSchema)
	table := "users"

	metaPath, err := SaveMetadataFile(spoolDir, table, fingerprint, serializedSchema)
	assert.NoError(t, err)

	// and loading metadata file back
	metadata, err := LoadMetadataFile(metaPath)
	assert.NoError(t, err)

	// and deserializing the schema
	loadedSchema, err := DeserializeSchema(metadata.Schema)
	assert.NoError(t, err)

	// then: all components work together correctly
	assert.NotNil(t, loadedSchema)
	assert.Equal(t, len(originalSchema.Fields()), len(loadedSchema.Fields()))
	assert.Equal(t, metadata.Table, table)
	assert.Equal(t, metadata.Fingerprint, fingerprint)

	// Verify the loaded schema matches the original
	for i, field := range originalSchema.Fields() {
		assert.Equal(t, field.Name, loadedSchema.Fields()[i].Name)
		assert.Equal(t, field.Type, loadedSchema.Fields()[i].Type)
	}
}
