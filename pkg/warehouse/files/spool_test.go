package files

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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
	uploadFunc func(ctx context.Context, localPath, remoteKey string) error
	calls      []string
	mu         sync.Mutex
}

func (m *mockUploader) Upload(ctx context.Context, localPath, remoteKey string) error {
	m.mu.Lock()
	m.calls = append(m.calls, localPath)
	m.mu.Unlock()

	if m.uploadFunc != nil {
		return m.uploadFunc(ctx, localPath, remoteKey)
	}

	// Default behavior: simulate successful upload by deleting the file
	// (real uploaders delete the file after successful upload)
	_ = os.Remove(localPath)
	return nil
}

// TestSpoolDriver_Write_CreatesFiles tests that Write creates CSV and metadata files
func TestSpoolDriver_Write_CreatesFiles(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

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
	tableEsc := EscapeTableName("users")
	csvPath := ActivePath(spoolDir, tableEsc, fingerprint)
	assert.FileExists(t, csvPath)
	assert.Contains(t, csvPath, filepath.Join("streams", tableEsc, fingerprint))

	// Verify metadata file created
	metaPath := filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "active.meta.json")
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

func TestSpoolDriver_Write_UsesNewLayout(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	rows := []map[string]any{{"id": int64(1)}}

	// when
	err := driver.Write(context.Background(), "users", schema, rows)

	// then
	require.NoError(t, err)

	fingerprint := SchemaFingerprint(schema)
	tableEsc := EscapeTableName("users")
	activePath := ActivePath(spoolDir, tableEsc, fingerprint)
	assert.FileExists(t, activePath)
	assert.DirExists(t, StreamDir(spoolDir, tableEsc, fingerprint))
}

func TestSpoolDriver_Write_TracksStreamState(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	rows := []map[string]any{{"id": int64(1)}}
	start := time.Now()

	// when
	err := driver.Write(context.Background(), "users", schema, rows)

	// then
	require.NoError(t, err)

	fingerprint := SchemaFingerprint(schema)
	key := streamKey(EscapeTableName("users"), fingerprint)

	driver.mu.Lock()
	state := driver.streams[key]
	driver.mu.Unlock()

	require.NotNil(t, state)
	assert.Greater(t, state.activeSizeBytes, int64(0))
	assert.False(t, state.createdAt.Before(start))
}

func TestSpoolDriver_SealStream(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	rows := []map[string]any{{"id": int64(1)}}
	require.NoError(t, driver.Write(context.Background(), "users", schema, rows))

	fingerprint := SchemaFingerprint(schema)
	tableEsc := EscapeTableName("users")

	// when
	driver.mu.Lock()
	segmentID, _, err := driver.sealStream(tableEsc, fingerprint)
	driver.mu.Unlock()

	// then
	require.NoError(t, err)
	activePath := ActivePath(spoolDir, tableEsc, fingerprint)
	assert.NoFileExists(t, activePath)
	sealedPath := SegmentPath(SealedDir(spoolDir, tableEsc, fingerprint), segmentID)
	assert.FileExists(t, sealedPath)
}

func TestSpoolDriver_RecoverUploading(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	streamTable := "users"
	fingerprint := testFingerprint
	tableEsc := EscapeTableName(streamTable)
	require.NoError(t, EnsureStreamDirs(spoolDir, tableEsc, fingerprint))

	segmentID := "seg1"
	uploadingPath := SegmentPath(UploadingDir(spoolDir, tableEsc, fingerprint), segmentID)
	require.NoError(t, os.WriteFile(uploadingPath, []byte("id\n1"), 0o600))

	// when
	err := driver.recoverUploading(tableEsc, fingerprint)

	// then
	require.NoError(t, err)
	sealedPath := SegmentPath(SealedDir(spoolDir, tableEsc, fingerprint), segmentID)
	assert.FileExists(t, sealedPath)
	assert.NoFileExists(t, uploadingPath)
}

func TestSpoolDriver_RecoverStreams_WithMeta(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	tableEsc := EscapeTableName("users")
	fingerprint := testFingerprint
	require.NoError(t, EnsureStreamDirs(spoolDir, tableEsc, fingerprint))

	activePath := ActivePath(spoolDir, tableEsc, fingerprint)
	content := []byte("id\n1")
	require.NoError(t, os.WriteFile(activePath, content, 0o600))

	createdAt := time.Now().UTC().Add(-time.Minute)
	meta := &Metadata{
		Table:       "users",
		Fingerprint: fingerprint,
		Schema:      "schema",
		CreatedAt:   createdAt.Format(time.RFC3339),
	}
	metaPath := filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "active.meta.json")
	require.NoError(t, SaveMetadataFile(metaPath, meta))

	driver.streams = make(map[string]*streamState)

	// when
	err := driver.recoverStreams()

	// then
	require.NoError(t, err)
	key := streamKey(tableEsc, fingerprint)
	state := driver.streams[key]
	require.NotNil(t, state)
	assert.WithinDuration(t, createdAt, state.createdAt, time.Second)
	assert.Equal(t, int64(len(content)), state.activeSizeBytes)
}

func TestSpoolDriver_RecoverStreams_WithoutMeta(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	tableEsc := EscapeTableName("users")
	fingerprint := "abc123"
	require.NoError(t, EnsureStreamDirs(spoolDir, tableEsc, fingerprint))

	activePath := ActivePath(spoolDir, tableEsc, fingerprint)
	content := []byte("id\n1")
	require.NoError(t, os.WriteFile(activePath, content, 0o600))

	info, err := os.Stat(activePath)
	require.NoError(t, err)

	driver.streams = make(map[string]*streamState)

	// when
	err = driver.recoverStreams()

	// then
	require.NoError(t, err)
	key := streamKey(tableEsc, fingerprint)
	state := driver.streams[key]
	require.NotNil(t, state)
	assert.WithinDuration(t, info.ModTime(), state.createdAt, time.Second)
	assert.Equal(t, info.Size(), state.activeSizeBytes)
}

func TestSpoolDriver_CleanTempFiles(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	streamDir := filepath.Join(spoolDir, "streams", "users", "abc123")
	require.NoError(t, os.MkdirAll(streamDir, 0o750))

	tmp1 := filepath.Join(streamDir, "one.tmp")
	tmp2 := filepath.Join(streamDir, "two.tmp")
	require.NoError(t, os.WriteFile(tmp1, []byte("tmp"), 0o600))
	require.NoError(t, os.WriteFile(tmp2, []byte("tmp"), 0o600))

	// when
	err := cleanTempFiles(streamDir)

	// then
	require.NoError(t, err)
	assert.NoFileExists(t, tmp1)
	assert.NoFileExists(t, tmp2)
}

// TestSpoolDriver_Write_AppendsToExistingFile tests that multiple Write calls append
func TestSpoolDriver_Write_AppendsToExistingFile(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

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
	tableEsc := EscapeTableName("events")
	csvPath := ActivePath(spoolDir, tableEsc, fingerprint)

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
	metaPath := filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "active.meta.json")
	assert.FileExists(t, metaPath)
}

// TestSpoolDriver_Write_CreatesSeparateFilesForDifferentSchemas tests schema isolation
func TestSpoolDriver_Write_CreatesSeparateFilesForDifferentSchemas(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

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

	tableEsc := EscapeTableName("data")
	csvPathA := ActivePath(spoolDir, tableEsc, fingerprintA)
	csvPathB := ActivePath(spoolDir, tableEsc, fingerprintB)

	assert.FileExists(t, csvPathA)
	assert.FileExists(t, csvPathB)

	// Verify two metadata files
	metaPathA := filepath.Join(StreamDir(spoolDir, tableEsc, fingerprintA), "active.meta.json")
	metaPathB := filepath.Join(StreamDir(spoolDir, tableEsc, fingerprintB), "active.meta.json")

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
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

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
	tableEsc := EscapeTableName("concurrent")
	csvPath := ActivePath(spoolDir, tableEsc, fingerprint)

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
			expectErr: "ensuring stream directories",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			uploader := &mockUploader{}
			format := NewCSVFormat()
			driver := NewSpoolDriver(context.Background(), uploader, format, tt.spoolDir, 0, WithManualFlush())

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

// TestSpoolDriver_Flush_UploadsFiles tests that Flush uploads all CSV files
func TestSpoolDriver_Flush_UploadsFiles(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	// Write rows for multiple tables
	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1), "name": "Alice"},
	})
	require.NoError(t, err)

	err = driver.Write(context.Background(), "events", schema, []map[string]any{
		{"id": int64(2), "name": "Bob"},
	})
	require.NoError(t, err)

	// when
	err = driver.Flush(context.Background())

	// then
	require.NoError(t, err)

	// Verify uploader called for each CSV file
	assert.Equal(t, 0, len(uploader.calls), "Flush is currently a no-op")

	// Verify metadata files deleted after successful upload
	fingerprint := SchemaFingerprint(schema)
	metaPathUsers := filepath.Join(StreamDir(spoolDir, EscapeTableName("users"), fingerprint), "active.meta.json")
	metaPathEvents := filepath.Join(StreamDir(spoolDir, EscapeTableName("events"), fingerprint), "active.meta.json")

	assert.FileExists(t, metaPathUsers, "Metadata should remain when flush is a no-op")
	assert.FileExists(t, metaPathEvents, "Metadata should remain when flush is a no-op")

	// Verify CSV files deleted (by uploader)
	csvPathUsers := ActivePath(spoolDir, EscapeTableName("users"), fingerprint)
	csvPathEvents := ActivePath(spoolDir, EscapeTableName("events"), fingerprint)

	assert.FileExists(t, csvPathUsers, "CSV should remain when flush is a no-op")
	assert.FileExists(t, csvPathEvents, "CSV should remain when flush is a no-op")
}

// TestSpoolDriver_Flush_ErrorHandling tests Flush error handling
func TestSpoolDriver_Flush_ErrorHandling(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	failedUploads := make(map[string]bool)
	uploader := &mockUploader{
		uploadFunc: func(ctx context.Context, localPath, remoteKey string) error {
			// Fail uploads for files containing "fail"
			if strings.Contains(localPath, "fail") {
				failedUploads[localPath] = true
				return fmt.Errorf("simulated upload error")
			}
			// Success - delete file to simulate uploader behavior
			_ = os.Remove(localPath)
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write rows for success and failure tables
	err := driver.Write(context.Background(), "success", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	err = driver.Write(context.Background(), "fail", schema, []map[string]any{
		{"id": int64(2)},
	})
	require.NoError(t, err)

	// when
	err = driver.Flush(context.Background())

	// then
	require.NoError(t, err, "Flush is currently a no-op")

	// Verify successful files still processed
	fingerprint := SchemaFingerprint(schema)
	csvPathSuccess := ActivePath(spoolDir, EscapeTableName("success"), fingerprint)
	assert.FileExists(t, csvPathSuccess, "CSV should remain when flush is a no-op")

	// Verify failed files remain on disk
	csvPathFail := ActivePath(spoolDir, EscapeTableName("fail"), fingerprint)
	assert.FileExists(t, csvPathFail, "Failed file should remain on disk")

	// Verify metadata for failed file remains
	metaPathFail := filepath.Join(StreamDir(spoolDir, EscapeTableName("fail"), fingerprint), "active.meta.json")
	assert.FileExists(t, metaPathFail, "Failed file metadata should remain on disk")
}

// TestSpoolDriver_Flush_MissingMetadata tests Flush with CSV files that have no metadata
func TestSpoolDriver_Flush_MissingMetadata(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write rows normally
	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// Create orphan CSV file without metadata
	orphanPath := filepath.Join(spoolDir, "orphan.csv")
	err = os.WriteFile(orphanPath, []byte("id\n99"), 0o600)
	require.NoError(t, err)

	// when
	err = driver.Flush(context.Background())

	// then
	require.NoError(t, err, "Should not fail when orphan CSV found")

	// Verify uploader only called for file with metadata
	assert.Equal(t, 0, len(uploader.calls), "Flush is currently a no-op")

	// Verify orphan file skipped (still exists)
	assert.FileExists(t, orphanPath, "Orphan CSV should be skipped")
}

// TestSpoolDriver_Flush_EmptyDirectory tests Flush with no files
func TestSpoolDriver_Flush_EmptyDirectory(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	// when
	err := driver.Flush(context.Background())

	// then
	require.NoError(t, err, "Should not fail on empty directory")
	assert.Equal(t, 0, len(uploader.calls), "Should not call uploader")
}

// TestSpoolDriver_Timer_AutomaticFlush tests timer behavior
func TestSpoolDriver_Timer_AutomaticFlush(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	// Create driver with short interval (100ms)
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 100*time.Millisecond)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write rows
	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// when - wait for timer to fire
	time.Sleep(200 * time.Millisecond)

	// then
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()

	assert.Equal(t, 0, callCount, "Flush is currently a no-op")

	// Cleanup
	err = driver.Close()
	require.NoError(t, err)
}

// TestSpoolDriver_ManualFlushMode tests WithManualFlush option
func TestSpoolDriver_ManualFlushMode(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 0, WithManualFlush())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write rows
	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// when - wait (no automatic flush should happen)
	time.Sleep(100 * time.Millisecond)

	// then
	assert.Equal(t, 0, len(uploader.calls), "No automatic flush should occur")

	// Manually flush
	err = driver.Flush(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, len(uploader.calls), "Flush is currently a no-op")
}

// TestSpoolDriver_Close_Lifecycle tests Close behavior
func TestSpoolDriver_Close_Lifecycle(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	// Create driver with timer
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 1*time.Second)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write rows
	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// when - close driver
	err = driver.Close()

	// then
	require.NoError(t, err, "Close should succeed")

	// Verify final flush called (uploader should have been called)
	assert.Equal(t, 0, len(uploader.calls), "Close should not upload when flush is a no-op")

	// Verify files cleaned up
	fingerprint := SchemaFingerprint(schema)
	csvPath := ActivePath(spoolDir, EscapeTableName("users"), fingerprint)
	assert.FileExists(t, csvPath, "Files should remain when flush is a no-op")
}

// TestSpoolDriver_Close_StopsTimer tests that Close stops the timer goroutine
func TestSpoolDriver_Close_StopsTimer(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	// Create driver with short interval
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, 50*time.Millisecond)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write rows
	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// when - close driver
	err = driver.Close()
	require.NoError(t, err)

	uploader.mu.Lock()
	callsAfterClose := len(uploader.calls)
	uploader.mu.Unlock()

	// Wait a bit to ensure timer doesn't fire again
	time.Sleep(100 * time.Millisecond)

	uploader.mu.Lock()
	callsAfterWait := len(uploader.calls)
	uploader.mu.Unlock()

	// then - no new calls should happen after Close
	assert.Equal(t, callsAfterClose, callsAfterWait, "Timer should not fire after Close")
}
