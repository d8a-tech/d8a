package files

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testFingerprint = "abc123"

// manualTicker is a test-only ticker whose ticks are driven programmatically.
type manualTicker struct {
	ch chan time.Time
}

func newManualTicker() *manualTicker {
	return &manualTicker{ch: make(chan time.Time, 1)}
}

func (m *manualTicker) C() <-chan time.Time { return m.ch }
func (m *manualTicker) Stop()               {}

// tick sends one tick, blocking until consumed or test times out.
func (m *manualTicker) tick() {
	m.ch <- time.Now()
}

// withManualTicker returns a SpoolOption that injects a manualTicker factory.
func withManualTicker(mt *manualTicker) SpoolOption {
	return func(sd *SpoolDriver) {
		sd.newTicker = func(d time.Duration) ticker { return mt }
	}
}

type uploadCall struct {
	localPath string
	remoteKey string
}

// mockUploader is a test double for Uploader interface
type mockUploader struct {
	uploadFunc func(ctx context.Context, localPath, remoteKey string) error
	calls      []uploadCall
	mu         sync.Mutex
}

func (m *mockUploader) Upload(ctx context.Context, localPath, remoteKey string) error {
	m.mu.Lock()
	m.calls = append(m.calls, uploadCall{localPath: localPath, remoteKey: remoteKey})
	m.mu.Unlock()

	if m.uploadFunc != nil {
		return m.uploadFunc(ctx, localPath, remoteKey)
	}

	// Success: driver is responsible for deleting the file, not the mock
	return nil
}

// TestSpoolDriver_Write_CreatesFiles tests that Write creates a CSV file with the expected content.
func TestSpoolDriver_Write_CreatesFiles(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

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

	// Verify active file created
	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName("users")
	filePath := activePath(spoolDir, tableEsc, fingerprint, "csv")
	assert.FileExists(t, filePath)
	assert.Contains(t, filePath, filepath.Join("streams", tableEsc, fingerprint))

	// Verify file contents
	fileData, err := os.ReadFile(filePath)
	require.NoError(t, err)
	fileContent := string(fileData)
	assert.Contains(t, fileContent, "id,name")
	assert.Contains(t, fileContent, "1,Alice")
	assert.Contains(t, fileContent, "2,Bob")
}

func TestSpoolDriver_Write_UsesNewLayout(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	rows := []map[string]any{{"id": int64(1)}}

	// when
	err := driver.Write(context.Background(), "users", schema, rows)

	// then
	require.NoError(t, err)

	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName("users")
	activePath := activePath(spoolDir, tableEsc, fingerprint, "csv")
	assert.FileExists(t, activePath)
	assert.DirExists(t, streamDir(spoolDir, tableEsc, fingerprint))
}

func TestSpoolDriver_Write_TracksStreamState(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	rows := []map[string]any{{"id": int64(1)}}
	start := time.Now()

	// when
	err := driver.Write(context.Background(), "users", schema, rows)

	// then
	require.NoError(t, err)

	fingerprint := schemaFingerprint(schema)
	key := streamKey(escapeTableName("users"), fingerprint)

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
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	rows := []map[string]any{{"id": int64(1)}}
	require.NoError(t, driver.Write(context.Background(), "users", schema, rows))

	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName("users")

	// when
	driver.mu.Lock()
	segmentID, _, err := driver.sealStream(tableEsc, fingerprint)
	driver.mu.Unlock()

	// then
	require.NoError(t, err)
	activePath := activePath(spoolDir, tableEsc, fingerprint, "csv")
	assert.NoFileExists(t, activePath)
	sealedPath := segmentPath(sealedDir(spoolDir, tableEsc, fingerprint), segmentID, "csv")
	assert.FileExists(t, sealedPath)
}

func TestSpoolDriver_RecoverUploading(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

	streamTable := "users"
	fingerprint := testFingerprint
	tableEsc := escapeTableName(streamTable)
	require.NoError(t, ensureStreamDirs(spoolDir, tableEsc, fingerprint))

	segmentID := "seg1"
	uploadingPath := segmentPath(uploadingDir(spoolDir, tableEsc, fingerprint), segmentID, "csv")
	require.NoError(t, os.WriteFile(uploadingPath, []byte("id\n1"), 0o600))

	// when
	err := driver.recoverUploading(tableEsc, fingerprint)

	// then
	require.NoError(t, err)
	sealedPath := segmentPath(sealedDir(spoolDir, tableEsc, fingerprint), segmentID, "csv")
	assert.FileExists(t, sealedPath)
	assert.NoFileExists(t, uploadingPath)
}

func TestSpoolDriver_RecoverStreams_UsesModTime(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

	tableEsc := escapeTableName("users")
	fingerprint := testFingerprint
	require.NoError(t, ensureStreamDirs(spoolDir, tableEsc, fingerprint))

	activePath := activePath(spoolDir, tableEsc, fingerprint, "csv")
	content := []byte("id\n1")
	require.NoError(t, os.WriteFile(activePath, content, 0o600))

	// Set a known modtime so the assertion is deterministic.
	knownTime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, os.Chtimes(activePath, knownTime, knownTime))

	driver.streams = make(map[string]*streamState)

	// when
	err := driver.recoverStreams()

	// then
	require.NoError(t, err)
	key := streamKey(tableEsc, fingerprint)
	state := driver.streams[key]
	require.NotNil(t, state)
	assert.WithinDuration(t, knownTime, state.createdAt, time.Second)
	assert.Equal(t, int64(len(content)), state.activeSizeBytes)
}

func TestSpoolDriver_DiscoverAllSealed_UsesFilenameTimestamp(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

	tableEsc := escapeTableName("users")
	fingerprint := testFingerprint
	require.NoError(t, ensureStreamDirs(spoolDir, tableEsc, fingerprint))

	// Place a timestamp-prefixed sealed segment directly on disk.
	knownSealTime := time.Date(2024, 2, 25, 10, 0, 0, 0, time.UTC)
	segmentID := segmentIDFromSealTime(knownSealTime)
	sealedPath := segmentPath(sealedDir(spoolDir, tableEsc, fingerprint), segmentID, "csv")
	require.NoError(t, os.WriteFile(sealedPath, []byte("id\n1"), 0o600))

	// when
	segments, err := driver.discoverAllSealed(nil)
	require.NoError(t, err)

	// then
	var recovered sealedSegment
	for _, seg := range segments {
		if seg.segmentID == segmentID {
			recovered = seg
			break
		}
	}
	require.NotEmpty(t, recovered.segmentID)
	assert.WithinDuration(t, knownSealTime, recovered.sealTime, time.Second)
}

func TestSpoolDriver_DiscoverAllSealed_LegacySegmentUsesModTime(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

	tableEsc := escapeTableName("users")
	fingerprint := testFingerprint
	require.NoError(t, ensureStreamDirs(spoolDir, tableEsc, fingerprint))

	// Legacy bare-UUID segment (no timestamp prefix).
	legacyID := "550e8400-e29b-41d4-a716-446655440000"
	sealedPath := segmentPath(sealedDir(spoolDir, tableEsc, fingerprint), legacyID, "csv")
	require.NoError(t, os.WriteFile(sealedPath, []byte("id\n1"), 0o600))

	knownModTime := time.Date(2023, 11, 14, 8, 0, 0, 0, time.UTC)
	require.NoError(t, os.Chtimes(sealedPath, knownModTime, knownModTime))

	// when
	segments, err := driver.discoverAllSealed(nil)
	require.NoError(t, err)

	// then
	var recovered sealedSegment
	for _, seg := range segments {
		if seg.segmentID == legacyID {
			recovered = seg
			break
		}
	}
	require.NotEmpty(t, recovered.segmentID)
	assert.WithinDuration(t, knownModTime, recovered.sealTime, time.Second)
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
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

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
	require.NoError(t, err1)

	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName("events")

	driver.mu.Lock()
	segmentID, _, sealErr := driver.sealStream(tableEsc, fingerprint)
	driver.mu.Unlock()
	require.NoError(t, sealErr)

	err2 := driver.Write(context.Background(), "events", schema, batch2)

	// then
	require.NoError(t, err2)

	sealedPath := segmentPath(sealedDir(spoolDir, tableEsc, fingerprint), segmentID, "csv")
	activePath := activePath(spoolDir, tableEsc, fingerprint, "csv")

	sealedFile, err := os.Open(sealedPath)
	require.NoError(t, err)
	defer func() {
		_ = sealedFile.Close()
	}()

	activeFile, err := os.Open(activePath)
	require.NoError(t, err)
	defer func() {
		_ = activeFile.Close()
	}()

	sealedRecords, err := csv.NewReader(sealedFile).ReadAll()
	require.NoError(t, err)
	activeRecords, err := csv.NewReader(activeFile).ReadAll()
	require.NoError(t, err)

	assert.Equal(t, 2, len(sealedRecords))
	assert.Equal(t, []string{"id", "value"}, sealedRecords[0])
	assert.Equal(t, []string{"1", "first"}, sealedRecords[1])

	assert.Equal(t, 2, len(activeRecords))
	assert.Equal(t, []string{"id", "value"}, activeRecords[0])
	assert.Equal(t, []string{"2", "second"}, activeRecords[1])
}

// TestSpoolDriver_Write_CreatesSeparateFilesForDifferentSchemas tests schema isolation
func TestSpoolDriver_Write_CreatesSeparateFilesForDifferentSchemas(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

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
	fingerprintA := schemaFingerprint(schemaA)
	fingerprintB := schemaFingerprint(schemaB)
	assert.NotEqual(t, fingerprintA, fingerprintB, "Different schemas should have different fingerprints")

	tableEsc := escapeTableName("data")
	filePathA := activePath(spoolDir, tableEsc, fingerprintA, "csv")
	filePathB := activePath(spoolDir, tableEsc, fingerprintB, "csv")

	assert.FileExists(t, filePathA)
	assert.FileExists(t, filePathB)

	// Verify each file contains only its schema's data
	fileDataA, err := os.ReadFile(filePathA)
	require.NoError(t, err)
	assert.Contains(t, string(fileDataA), "field_a")
	assert.NotContains(t, string(fileDataA), "field_b")

	fileDataB, err := os.ReadFile(filePathB)
	require.NoError(t, err)
	assert.Contains(t, string(fileDataB), "field_b")
	assert.NotContains(t, string(fileDataB), "field_a")
}

// TestSpoolDriver_Write_ConcurrentWrites tests mutex protection for concurrent writes
func TestSpoolDriver_Write_ConcurrentWrites(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

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
	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName("concurrent")
	csvPath := activePath(spoolDir, tableEsc, fingerprint, "csv")

	csvData, err := os.ReadFile(csvPath)
	require.NoError(t, err)
	fileContent := string(csvData)

	// Verify header appears exactly once
	headerCount := strings.Count(fileContent, "id\n")
	assert.Equal(t, 1, headerCount, "Header should appear exactly once")

	// Verify total row count (header + data rows)
	lines := strings.Split(strings.TrimSpace(fileContent), "\n")
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
			driver := NewSpoolDriver(context.Background(), uploader, format, tt.spoolDir, withManualCycle())

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

// TestSpoolDriver_RunFlushCycle_UploadsFiles tests that runFlushCycle uploads sealed segments
func TestSpoolDriver_RunFlushCycle_UploadsFiles(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

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

	// when - force seal all active segments
	err = driver.runFlushCycle(context.Background(), true)

	// then
	require.NoError(t, err)

	// Verify uploader called for each sealed segment
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 2, callCount, "Should upload 2 segments")
}

// TestSpoolDriver_RunFlushCycle_ErrorHandling tests runFlushCycle error handling
func TestSpoolDriver_RunFlushCycle_ErrorHandling(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploadAttempts := 0
	uploader := &mockUploader{
		uploadFunc: func(ctx context.Context, localPath, remoteKey string) error {
			uploadAttempts++
			return fmt.Errorf("simulated upload error")
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	err := driver.Write(context.Background(), "test", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// when
	err = driver.runFlushCycle(context.Background(), true)

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated upload error")
	assert.Equal(t, 1, uploadAttempts)
}

// TestSpoolDriver_RunFlushCycle_EmptyDirectory tests runFlushCycle with no files
func TestSpoolDriver_RunFlushCycle_EmptyDirectory(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())

	// when
	err := driver.runFlushCycle(context.Background(), false)

	// then
	require.NoError(t, err, "Should not fail on empty directory")
	assert.Equal(t, 0, len(uploader.calls), "Should not call uploader")
}

// TestSpoolDriver_Close_Lifecycle tests Close behavior
func TestSpoolDriver_Close_Lifecycle(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	// Create driver with timer
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		WithSealCheckInterval(1*time.Second))

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

	// Verify final flush called (uploader should have been called - Close does forceAll)
	assert.Equal(t, 1, len(uploader.calls), "Close should upload active segment")

	// Verify active file is gone (sealed and uploaded)
	fingerprint := schemaFingerprint(schema)
	csvPath := activePath(spoolDir, escapeTableName("users"), fingerprint, "csv")
	assert.NoFileExists(t, csvPath, "Active file should be sealed and uploaded")
}

// TestSpoolDriver_SizeTrigger tests that segments are sealed when size exceeds maxSegmentSize
func TestSpoolDriver_SizeTrigger(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithMaxSegmentSize(50)) // Very small size threshold

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	// Write enough data to exceed threshold
	rows := []map[string]any{
		{"id": int64(1), "value": "this is a long string to exceed the tiny threshold"},
	}
	err := driver.Write(context.Background(), "users", schema, rows)
	require.NoError(t, err)

	// when - run flush cycle (NOT forced)
	err = driver.runFlushCycle(context.Background(), false)

	// then
	require.NoError(t, err)

	// Verify upload happened due to size trigger
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 1, callCount, "Should upload segment that exceeded size threshold")
}

// TestSpoolDriver_AgeTrigger tests that segments are sealed when age exceeds maxSegmentAge
func TestSpoolDriver_AgeTrigger(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithMaxSegmentAge(1*time.Hour))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// Manually set createdAt to simulate old segment
	fingerprint := schemaFingerprint(schema)
	key := streamKey(escapeTableName("users"), fingerprint)
	driver.mu.Lock()
	driver.streams[key].createdAt = time.Now().Add(-2 * time.Hour)
	driver.mu.Unlock()

	// when - run flush cycle (NOT forced)
	err = driver.runFlushCycle(context.Background(), false)

	// then
	require.NoError(t, err)

	// Verify upload happened due to age trigger
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 1, callCount, "Should upload segment that exceeded age threshold")
}

// TestSpoolDriver_NoTrigger tests that segments are NOT sealed when neither trigger fires
func TestSpoolDriver_NoTrigger(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithMaxSegmentSize(1<<30),    // 1 GiB - very large
		WithMaxSegmentAge(time.Hour)) // 1 hour - not reached

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// when - run flush cycle (NOT forced)
	err = driver.runFlushCycle(context.Background(), false)

	// then
	require.NoError(t, err)

	// Verify NO upload happened
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 0, callCount, "Should NOT upload when no trigger fires")

	// Verify active file still exists
	fingerprint := schemaFingerprint(schema)
	activePath := activePath(spoolDir, escapeTableName("users"), fingerprint, "csv")
	assert.FileExists(t, activePath, "Active file should still exist")
}

// TestSpoolDriver_ForceAll_OnClose tests that Close forces seal and upload of all segments
func TestSpoolDriver_ForceAll_OnClose(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithMaxSegmentSize(1<<30),    // Large threshold
		WithMaxSegmentAge(time.Hour)) // Long age

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write small amount of data (below threshold)
	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// when - close driver
	err = driver.Close()

	// then
	require.NoError(t, err)

	// Verify upload happened (forced seal)
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 1, callCount, "Close should force seal and upload")
}

// TestSpoolDriver_FailureCounter tests failure counting and quarantine
func TestSpoolDriver_FailureCounter(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{
		uploadFunc: func(ctx context.Context, localPath, remoteKey string) error {
			return fmt.Errorf("persistent upload error")
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	err := driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName("users")
	streamDir := streamDir(spoolDir, tableEsc, fingerprint)
	sealedDir := sealedDir(spoolDir, tableEsc, fingerprint)
	failedDir := failedDir(spoolDir, tableEsc, fingerprint)

	// when - first failure
	_ = driver.runFlushCycle(context.Background(), true)

	// then - verify failcount = 1
	entries, err := os.ReadDir(sealedDir)
	require.NoError(t, err)
	var segmentID string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".csv") {
			segmentID = strings.TrimSuffix(e.Name(), ".csv")
			break
		}
	}
	require.NotEmpty(t, segmentID, "Should have a sealed segment")

	fc := readFailCount(streamDir, segmentID)
	assert.Equal(t, 1, fc, "Fail count should be 1 after first failure")

	// when - second failure
	_ = driver.runFlushCycle(context.Background(), false)
	fc = readFailCount(streamDir, segmentID)
	assert.Equal(t, 2, fc, "Fail count should be 2 after second failure")

	// when - third failure (should quarantine)
	_ = driver.runFlushCycle(context.Background(), false)

	// then - verify segment moved to failed/
	failedPath := segmentPath(failedDir, segmentID, "csv")
	assert.FileExists(t, failedPath, "Segment should be quarantined after 3 failures")

	sealedPath := segmentPath(sealedDir, segmentID, "csv")
	assert.NoFileExists(t, sealedPath, "Segment should NOT be in sealed after quarantine")

	// Failcount file should be removed
	failCountPath := failCountPath(streamDir, segmentID)
	assert.NoFileExists(t, failCountPath, "Failcount file should be removed after quarantine")
}

// TestSpoolDriver_RemoteKeyFormat tests that the remote key matches expected pattern
func TestSpoolDriver_RemoteKeyFormat(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedRemoteKey string
	uploader := &mockUploader{
		uploadFunc: func(ctx context.Context, localPath, remoteKey string) error {
			capturedRemoteKey = remoteKey
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	err := driver.Write(context.Background(), "my_table", schema, []map[string]any{
		{"id": int64(1)},
	})
	require.NoError(t, err)

	// when
	err = driver.runFlushCycle(context.Background(), true)
	require.NoError(t, err)

	// then - verify remote key format
	// Pattern: table=<tableEsc>/schema=<fp>/dt=<YYYY>/<MM>/<DD>/<unixSeconds>_<uuid>.csv
	pattern := `^table=my_table/schema=[a-f0-9]+/dt=\d{4}/\d{2}/\d{2}/\d+_[a-f0-9\-]+\.csv$`
	matched, err := regexp.MatchString(pattern, capturedRemoteKey)
	require.NoError(t, err)
	assert.True(t, matched, "Remote key should match pattern, got: %s", capturedRemoteKey)
}

// TestSpoolDriver_Timer_Deterministic tests timer-based flush using deterministic ticker injection.
func TestSpoolDriver_Timer_Deterministic(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedCSV []byte
	var capturedMu sync.Mutex
	uploader := &mockUploader{
		uploadFunc: func(ctx context.Context, localPath, remoteKey string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedMu.Lock()
			capturedCSV = data
			capturedMu.Unlock()
			return nil
		},
	}
	format := NewCSVFormat()
	mt := newManualTicker()

	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		WithMaxSegmentSize(1), // tiny so every cycle seals
		WithSealCheckInterval(time.Hour),
		withManualTicker(mt),
	)
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "test", schema, []map[string]any{
		{"id": int64(42)},
	}))

	// when
	mt.tick()

	// then
	require.Eventually(t, func() bool {
		uploader.mu.Lock()
		defer uploader.mu.Unlock()
		return len(uploader.calls) == 1
	}, 1*time.Second, 5*time.Millisecond, "Expected exactly 1 upload call")

	// Verify file contents
	capturedMu.Lock()
	csvData := capturedCSV
	capturedMu.Unlock()
	require.NotEmpty(t, csvData)
	reader := csv.NewReader(strings.NewReader(string(csvData)))
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2) // header + 1 data row
	assert.Equal(t, "id", records[0][0])
	assert.Equal(t, "42", records[1][0])
}

// TestSpoolDriver_Close_Idempotent tests that Close is safe to call multiple times.
func TestSpoolDriver_Close_Idempotent(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()

	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
	)

	// when - call Close twice
	err1 := driver.Close()
	err2 := driver.Close()

	// then - neither panics and both return nil
	require.NoError(t, err1)
	require.NoError(t, err2)
}

// TestSpoolDriver_Rotation_SealedFileImmutable tests that sealing creates an immutable file.
func TestSpoolDriver_Rotation_SealedFileImmutable(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	tableEsc := escapeTableName("users")
	fp := schemaFingerprint(schema)

	// Write batch1
	require.NoError(t, driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	}))

	// Seal manually
	driver.mu.Lock()
	segID1, _, err := driver.sealStream(tableEsc, fp)
	driver.mu.Unlock()
	require.NoError(t, err)

	// Write batch2
	require.NoError(t, driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(2)},
	}))

	// then
	sealedPath := segmentPath(sealedDir(spoolDir, tableEsc, fp), segID1, "csv")
	activePath := activePath(spoolDir, tableEsc, fp, "csv")

	assert.FileExists(t, sealedPath)
	assert.FileExists(t, activePath)
	assert.NotEqual(t, sealedPath, activePath)

	// Read sealed CSV
	sealedData, err := os.ReadFile(sealedPath)
	require.NoError(t, err)
	sealedContent := string(sealedData)
	assert.Contains(t, sealedContent, "id")
	assert.Contains(t, sealedContent, "1")
	assert.NotContains(t, sealedContent, "2")

	// Read active CSV
	activeData, err := os.ReadFile(activePath)
	require.NoError(t, err)
	activeContent := string(activeData)
	assert.Contains(t, activeContent, "id")
	assert.Contains(t, activeContent, "2")
	assert.NotContains(t, activeContent, "\n1,")
	assert.NotContains(t, activeContent, "\n1\n")
}

// TestSpoolDriver_Immutability_TwoSealsDistinctKeys tests that two seals produce distinct segment IDs.
func TestSpoolDriver_Immutability_TwoSealsDistinctKeys(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	tableEsc := escapeTableName("users")
	fp := schemaFingerprint(schema)

	// Write row1, seal
	require.NoError(t, driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	}))
	driver.mu.Lock()
	segID1, _, err := driver.sealStream(tableEsc, fp)
	driver.mu.Unlock()
	require.NoError(t, err)

	// Write row2, seal
	require.NoError(t, driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(2)},
	}))
	driver.mu.Lock()
	segID2, _, err := driver.sealStream(tableEsc, fp)
	driver.mu.Unlock()
	require.NoError(t, err)

	// then
	assert.NotEqual(t, segID1, segID2)

	// Upload both sealed segments
	require.NoError(t, driver.runFlushCycle(context.Background(), false))

	uploader.mu.Lock()
	calls := uploader.calls
	uploader.mu.Unlock()

	require.Len(t, calls, 2)
	assert.NotEqual(t, calls[0].remoteKey, calls[1].remoteKey)
}

// TestSpoolDriver_Quarantine_StopsRetrying tests that quarantined segments are not retried.
func TestSpoolDriver_Quarantine_StopsRetrying(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{
		uploadFunc: func(ctx context.Context, localPath, remoteKey string) error {
			return fmt.Errorf("persistent upload error")
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	}))

	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName("users")
	failedDir := failedDir(spoolDir, tableEsc, fingerprint)

	// when - run 3 times to reach quarantine
	_ = driver.runFlushCycle(context.Background(), true)
	_ = driver.runFlushCycle(context.Background(), false)
	_ = driver.runFlushCycle(context.Background(), false)

	// then - segment should be in failed/
	entries, err := os.ReadDir(failedDir)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 1)

	uploader.mu.Lock()
	callsAfter3 := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 3, callsAfter3)

	// when - run a 4th time
	_ = driver.runFlushCycle(context.Background(), false)

	// then - uploader was NOT called a 4th time
	uploader.mu.Lock()
	callsAfter4 := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 3, callsAfter4)
}

// TestCSVTimestamp_RFC3339Nano tests that timestamps are written with nanosecond precision.
func TestCSVTimestamp_RFC3339Nano(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedContent []byte
	uploader := &mockUploader{
		uploadFunc: func(ctx context.Context, localPath, remoteKey string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedContent = data
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir, withManualCycle())
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ns},
	}, nil)

	ts := time.Date(2026, 2, 24, 14, 30, 45, 123456789, time.UTC)
	require.NoError(t, driver.Write(context.Background(), "events", schema, []map[string]any{
		{"ts": ts},
	}))

	// when
	require.NoError(t, driver.runFlushCycle(context.Background(), true))

	// then
	require.NotEmpty(t, capturedContent)

	reader := csv.NewReader(strings.NewReader(string(capturedContent)))
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2) // header + 1 row

	assert.Equal(t, "ts", records[0][0])
	assert.Equal(t, "2026-02-24T14:30:45.123456789Z", records[1][0])
}

// TestSpoolDriver_Concurrency_WritesDuringTickSeal tests concurrent writes during flush cycles.
func TestSpoolDriver_Concurrency_WritesDuringTickSeal(t *testing.T) {
	// given
	spoolDir := t.TempDir()

	type capturedFile struct {
		data []byte
	}
	var capturedFiles []capturedFile
	var capturedMu sync.Mutex

	uploader := &mockUploader{
		uploadFunc: func(ctx context.Context, localPath, remoteKey string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedMu.Lock()
			capturedFiles = append(capturedFiles, capturedFile{data: data})
			capturedMu.Unlock()
			return nil
		},
	}
	format := NewCSVFormat()
	ctx := context.Background()

	driver := NewSpoolDriver(ctx, uploader, format, spoolDir,
		withManualCycle(),
		WithMaxSegmentSize(1), // tiny so every flush cycle seals
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	const numWriters = 5
	const rowsPerWriter = 20

	var writersWg sync.WaitGroup
	writersDone := make(chan struct{})

	// when - start writer goroutines
	for i := 0; i < numWriters; i++ {
		writersWg.Add(1)
		go func(writerID int) {
			defer writersWg.Done()
			for j := 0; j < rowsPerWriter; j++ {
				err := driver.Write(ctx, "events", schema, []map[string]any{
					{"id": int64(writerID*rowsPerWriter + j)},
				})
				assert.NoError(t, err)
			}
		}(i)
	}

	// Start flusher goroutine
	var flusherWg sync.WaitGroup
	flusherWg.Add(1)
	go func() {
		defer flusherWg.Done()
		for {
			select {
			case <-writersDone:
				return
			default:
				_ = driver.runFlushCycle(ctx, false)
				runtime.Gosched()
			}
		}
	}()

	// Wait for writers to finish
	writersWg.Wait()
	close(writersDone)
	flusherWg.Wait()

	// Drain remainder
	require.NoError(t, driver.runFlushCycle(ctx, true))

	// then - count total data rows across all captured files
	capturedMu.Lock()
	files := capturedFiles
	capturedMu.Unlock()

	totalDataRows := 0
	for _, f := range files {
		reader := csv.NewReader(strings.NewReader(string(f.data)))
		records, err := reader.ReadAll()
		require.NoError(t, err)
		// Each file has 1 header row + N data rows
		if len(records) > 1 {
			totalDataRows += len(records) - 1
		}
	}

	assert.Equal(t, numWriters*rowsPerWriter, totalDataRows,
		"Total data rows should equal numWriters * rowsPerWriter")

	// Verify no .csv files remain in the sealed dir
	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName("events")
	sealedDir := sealedDir(spoolDir, tableEsc, fingerprint)
	entries, err := os.ReadDir(sealedDir)
	if err != nil && !os.IsNotExist(err) {
		require.NoError(t, err)
	}
	for _, entry := range entries {
		assert.False(t, strings.HasSuffix(entry.Name(), ".csv"),
			"No .csv files should remain in sealed dir, found: %s", entry.Name())
	}
}
