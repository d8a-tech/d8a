package files

import (
	"context"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestSegmentRemoteKey(t *testing.T) {
	tests := []struct {
		name        string
		template    string
		tableEsc    string
		fingerprint string
		segmentID   string
		ext         string
		sealTime    time.Time
		expected    string
		expectError bool
	}{
		{
			name: "default template (matches current behavior)",
			template: "table={{.Table}}/schema={{.Schema}}/dt={{.Year}}/{{.MonthPadded}}/" +
				"{{.DayPadded}}/{{.SegmentID}}.{{.Extension}}",
			tableEsc:    "events",
			fingerprint: "abc123",
			segmentID:   "seg-1",
			ext:         "csv",
			sealTime:    time.Date(2026, time.February, 25, 10, 2, 3, 0, time.UTC),
			expected:    "table=events/schema=abc123/dt=2026/02/25/seg-1.csv",
			expectError: false,
		},
		{
			name: "hive-style partitioning",
			template: "table={{.Table}}/year={{.Year}}/month={{.MonthPadded}}/day={{.DayPadded}}/" +
				"{{.SegmentID}}.{{.Extension}}",
			tableEsc:    "events",
			fingerprint: "abc123",
			segmentID:   "seg-1",
			ext:         "csv",
			sealTime:    time.Date(2026, time.February, 25, 10, 2, 3, 0, time.UTC),
			expected:    "table=events/year=2026/month=02/day=25/seg-1.csv",
			expectError: false,
		},
		{
			name:        "flat structure",
			template:    "{{.Table}}_{{.Year}}{{.MonthPadded}}{{.DayPadded}}_{{.SegmentID}}.{{.Extension}}",
			tableEsc:    "events",
			fingerprint: "abc123",
			segmentID:   "seg-1",
			ext:         "csv",
			sealTime:    time.Date(2026, time.February, 25, 10, 2, 3, 0, time.UTC),
			expected:    "events_20260225_seg-1.csv",
			expectError: false,
		},
		{
			name:        "extension only",
			template:    "{{.SegmentID}}.{{.Extension}}",
			tableEsc:    "events",
			fingerprint: "abc123",
			segmentID:   "seg-1",
			ext:         "csv",
			sealTime:    time.Date(2026, time.February, 25, 10, 2, 3, 0, time.UTC),
			expected:    "seg-1.csv",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			tmpl, err := template.New("path").Parse(tt.template)
			require.NoError(t, err)

			// when
			key, err := segmentRemoteKey(tmpl, tt.tableEsc, tt.fingerprint, tt.segmentID, tt.ext, tt.sealTime)

			// then
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, key)
			}
		})
	}
}

// mockUploaderForTest is a test double for Uploader interface
type mockUploaderForTest struct {
	calls []struct {
		localPath string
		remoteKey string
	}
	mu sync.Mutex
}

func (m *mockUploaderForTest) Upload(ctx context.Context, localPath, remoteKey string) error {
	m.mu.Lock()
	m.calls = append(m.calls, struct {
		localPath string
		remoteKey string
	}{localPath: localPath, remoteKey: remoteKey})
	m.mu.Unlock()
	return nil
}

// TestSpoolDriverPathTemplate verifies end-to-end template usage in path generation.
func TestSpoolDriverPathTemplate(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	ctx := context.Background()
	uploader := &mockUploaderForTest{}
	customTemplate := "custom/{{.Table}}/year={{.Year}}/{{.SegmentID}}.{{.Extension}}"

	driver := NewSpoolDriver(
		ctx,
		uploader,
		NewCSVFormat(),
		spoolDir,
		WithPathTemplate(customTemplate),
		withManualCycle(),
		WithFlushOnClose(true),
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// when
	err := driver.Write(ctx, "events", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// Trigger flush by closing with flush-on-close
	err = driver.Close()
	require.NoError(t, err)

	// then
	uploader.mu.Lock()
	defer uploader.mu.Unlock()

	// Verify upload happened
	assert.Equal(t, 1, len(uploader.calls), "Should upload exactly one segment")

	// Verify remote key matches custom template format
	remoteKey := uploader.calls[0].remoteKey
	assert.Contains(t, remoteKey, "custom/events/year=", "Path should start with custom/events/year=")
	assert.Contains(t, remoteKey, ".csv", "Path should end with .csv extension")
	assert.Contains(t, remoteKey, "year=2026", "Path should contain year=2026 (current year in test)")
}
