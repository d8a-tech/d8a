package files

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSpoolDriverImplementsDriver verifies that spoolDriver implements warehouse.Driver
func TestSpoolDriverImplementsDriver(t *testing.T) {
	var _ warehouse.Driver = (*spoolDriver)(nil)
}

// mockUploader is a test double for Uploader interface
type mockUploader struct {
	uploadFunc func(ctx context.Context, filePath string) error
	calls      []string
	mu         sync.Mutex
}

func (m *mockUploader) Upload(ctx context.Context, filePath string) error {
	m.mu.Lock()
	m.calls = append(m.calls, filePath)
	m.mu.Unlock()

	if m.uploadFunc != nil {
		return m.uploadFunc(ctx, filePath)
	}
	return nil
}

// TestSpoolDriver_Write_CreatesFiles tests that Write creates CSV and metadata files
func TestSpoolDriver_Write_CreatesFiles(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	rows := []map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}

	// when
	err := driver.Write(context.Background(), "users", schema, rows)

	// then
	require.NoError(t, err)

	// Verify CSV file created
	fingerprint := SchemaFingerprint(schema)
	csvFile := FilenameForWrite("users", fingerprint, format)
	csvPath := filepath.Join(spoolDir, csvFile)
	assert.FileExists(t, csvPath)

	// Verify metadata file created
	metaFile := MetadataFilename("users", fingerprint)
	metaPath := filepath.Join(spoolDir, metaFile)
	assert.FileExists(t, metaPath)

	// Verify CSV contents
	csvData, err := os.ReadFile(csvPath)
	require.NoError(t, err)
	csvContent := string(csvData)
	assert.Contains(t, csvContent, "id,name")
	assert.Contains(t, csvContent, "1,Alice")
	assert.Contains(t, csvContent, "2,Bob")

	// Verify metadata contents
	metaFile2, err := os.Open(metaPath)
	require.NoError(t, err)
	defer func() {
		_ = metaFile2.Close()
	}()

	meta, err := ReadMetadata(metaFile2)
	require.NoError(t, err)
	assert.Equal(t, "users", meta.Table)
	assert.Equal(t, fingerprint, meta.Fingerprint)
	assert.NotEmpty(t, meta.Schema)
	assert.NotEmpty(t, meta.CreatedAt)
}

// TestSpoolDriver_Write_AppendsToExistingFile tests that multiple Write calls append
func TestSpoolDriver_Write_AppendsToExistingFile(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	batch1 := []map[string]any{
		{"id": int64(1), "value": "first"},
	}
	batch2 := []map[string]any{
		{"id": int64(2), "value": "second"},
	}

	// when
	err1 := driver.Write(context.Background(), "events", schema, batch1)
	err2 := driver.Write(context.Background(), "events", schema, batch2)

	// then
	require.NoError(t, err1)
	require.NoError(t, err2)

	// Verify single CSV file contains both batches
	fingerprint := SchemaFingerprint(schema)
	csvFile := FilenameForWrite("events", fingerprint, format)
	csvPath := filepath.Join(spoolDir, csvFile)

	csvData, err := os.ReadFile(csvPath)
	require.NoError(t, err)
	csvContent := string(csvData)

	// Verify header only appears once
	headerCount := strings.Count(csvContent, "id,value")
	assert.Equal(t, 1, headerCount, "Header should appear exactly once")

	// Verify both batches present
	assert.Contains(t, csvContent, "1,first")
	assert.Contains(t, csvContent, "2,second")

	// Verify metadata created only once
	metaFile := MetadataFilename("events", fingerprint)
	metaPath := filepath.Join(spoolDir, metaFile)
	assert.FileExists(t, metaPath)
}

// TestSpoolDriver_Write_CreatesSeparateFilesForDifferentSchemas tests schema isolation
func TestSpoolDriver_Write_CreatesSeparateFilesForDifferentSchemas(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(uploader, format, spoolDir, 0, WithManualFlush())

	schemaA := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "field_a", Type: arrow.BinaryTypes.String},
	}, nil)

	schemaB := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "field_b", Type: arrow.BinaryTypes.String},
	}, nil)

	rowsA := []map[string]any{{"id": int64(1), "field_a": "alpha"}}
	rowsB := []map[string]any{{"id": int64(2), "field_b": "beta"}}

	// when
	err1 := driver.Write(context.Background(), "data", schemaA, rowsA)
	err2 := driver.Write(context.Background(), "data", schemaB, rowsB)

	// then
	require.NoError(t, err1)
	require.NoError(t, err2)

	// Verify two CSV files created (different fingerprints)
	fingerprintA := SchemaFingerprint(schemaA)
	fingerprintB := SchemaFingerprint(schemaB)
	assert.NotEqual(t, fingerprintA, fingerprintB, "Different schemas should have different fingerprints")

	csvFileA := FilenameForWrite("data", fingerprintA, format)
	csvFileB := FilenameForWrite("data", fingerprintB, format)

	csvPathA := filepath.Join(spoolDir, csvFileA)
	csvPathB := filepath.Join(spoolDir, csvFileB)

	assert.FileExists(t, csvPathA)
	assert.FileExists(t, csvPathB)

	// Verify two metadata files
	metaPathA := filepath.Join(spoolDir, MetadataFilename("data", fingerprintA))
	metaPathB := filepath.Join(spoolDir, MetadataFilename("data", fingerprintB))

	assert.FileExists(t, metaPathA)
	assert.FileExists(t, metaPathB)

	// Verify each file contains only its schema's data
	csvDataA, err := os.ReadFile(csvPathA)
	require.NoError(t, err)
	assert.Contains(t, string(csvDataA), "field_a")
	assert.NotContains(t, string(csvDataA), "field_b")

	csvDataB, err := os.ReadFile(csvPathB)
	require.NoError(t, err)
	assert.Contains(t, string(csvDataB), "field_b")
	assert.NotContains(t, string(csvDataB), "field_a")
}

// TestSpoolDriver_Write_ConcurrentWrites tests mutex protection for concurrent writes
func TestSpoolDriver_Write_ConcurrentWrites(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	numGoroutines := 10
	rowsPerGoroutine := 5

	// when - launch concurrent writes
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rows := make([]map[string]any, rowsPerGoroutine)
			for j := 0; j < rowsPerGoroutine; j++ {
				rows[j] = map[string]any{"id": int64(idx*rowsPerGoroutine + j)}
			}
			err := driver.Write(context.Background(), "concurrent", schema, rows)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// then
	fingerprint := SchemaFingerprint(schema)
	csvFile := FilenameForWrite("concurrent", fingerprint, format)
	csvPath := filepath.Join(spoolDir, csvFile)

	csvData, err := os.ReadFile(csvPath)
	require.NoError(t, err)
	csvContent := string(csvData)

	// Verify header appears exactly once
	headerCount := strings.Count(csvContent, "id\n")
	assert.Equal(t, 1, headerCount, "Header should appear exactly once")

	// Verify total row count (header + data rows)
	lines := strings.Split(strings.TrimSpace(csvContent), "\n")
	expectedRows := 1 + (numGoroutines * rowsPerGoroutine) // header + data rows
	assert.Equal(t, expectedRows, len(lines), "All rows should be written")
}

// TestSpoolDriver_Write_ErrorHandling tests error cases
func TestSpoolDriver_Write_ErrorHandling(t *testing.T) {
	tests := []struct {
		name      string
		spoolDir  string
		expectErr string
	}{
		{
			name:      "invalid spool directory",
			spoolDir:  "/nonexistent/invalid/path",
			expectErr: "opening CSV file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			uploader := &mockUploader{}
			format := NewCSVFormat()
			driver := NewSpoolDriver(uploader, format, tt.spoolDir, 0, WithManualFlush())

			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			}, nil)

			rows := []map[string]any{{"id": int64(1)}}

			// when
			err := driver.Write(context.Background(), "test", schema, rows)

			// then
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectErr)
		})
	}
}
