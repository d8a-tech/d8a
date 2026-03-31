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

	return nil
}

// TestSpoolDriver_Write_AndFlush tests that Write + forced flush uploads formatted CSV data.
func TestSpoolDriver_Write_AndFlush(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedCSV []byte
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, localPath, _ string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedCSV = data
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))

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
	require.NoError(t, err)

	err = driver.Close()
	require.NoError(t, err)

	// then
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 1, callCount, "Should upload exactly one segment")

	require.NotEmpty(t, capturedCSV)
	reader := csv.NewReader(strings.NewReader(string(capturedCSV)))
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 3) // header + 2 data rows
	assert.Equal(t, []string{"id", "name"}, records[0])
	assert.Equal(t, "1", records[1][0])
	assert.Equal(t, "Alice", records[1][1])
	assert.Equal(t, "2", records[2][0])
	assert.Equal(t, "Bob", records[2][1])
}

// TestSpoolDriver_Write_SchemaFingerprintIsolation tests that different schemas
// produce separate uploads.
func TestSpoolDriver_Write_SchemaFingerprintIsolation(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedFiles [][]byte
	var capturedMu sync.Mutex
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, localPath, _ string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedMu.Lock()
			capturedFiles = append(capturedFiles, data)
			capturedMu.Unlock()
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))

	schemaA := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "field_a", Type: arrow.BinaryTypes.String},
	}, nil)

	schemaB := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "field_b", Type: arrow.BinaryTypes.String},
	}, nil)

	// when
	require.NoError(t, driver.Write(context.Background(), "data", schemaA,
		[]map[string]any{{"id": int64(1), "field_a": "alpha"}}))
	require.NoError(t, driver.Write(context.Background(), "data", schemaB,
		[]map[string]any{{"id": int64(2), "field_b": "beta"}}))

	err := driver.Close()
	require.NoError(t, err)

	// then
	capturedMu.Lock()
	files := capturedFiles
	capturedMu.Unlock()
	require.Len(t, files, 2, "Should produce 2 separate uploads for 2 schemas")

	// Check that each file contains only its own schema's data
	var containsA, containsB bool
	for _, f := range files {
		s := string(f)
		if strings.Contains(s, "field_a") {
			containsA = true
			assert.NotContains(t, s, "field_b")
		}
		if strings.Contains(s, "field_b") {
			containsB = true
			assert.NotContains(t, s, "field_a")
		}
	}
	assert.True(t, containsA, "Should have a file with field_a")
	assert.True(t, containsB, "Should have a file with field_b")
}

// TestSpoolDriver_Write_ConcurrentWrites tests mutex protection for concurrent writes.
func TestSpoolDriver_Write_ConcurrentWrites(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedFiles [][]byte
	var capturedMu sync.Mutex
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, localPath, _ string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedMu.Lock()
			capturedFiles = append(capturedFiles, data)
			capturedMu.Unlock()
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))

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

	err := driver.Close()
	require.NoError(t, err)

	// then - count total data rows across all captured files
	capturedMu.Lock()
	files := capturedFiles
	capturedMu.Unlock()

	totalDataRows := 0
	for _, f := range files {
		reader := csv.NewReader(strings.NewReader(string(f)))
		records, err := reader.ReadAll()
		require.NoError(t, err)
		if len(records) > 1 {
			totalDataRows += len(records) - 1
		}
	}

	assert.Equal(t, numGoroutines*rowsPerGoroutine, totalDataRows,
		"Total data rows should equal numWriters * rowsPerGoroutine")
}

// TestSpoolDriver_Timer_Deterministic tests timer-based flush using deterministic ticker injection.
func TestSpoolDriver_Timer_Deterministic(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedCSV []byte
	var capturedMu sync.Mutex
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, localPath, _ string) error {
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
	}, 2*time.Second, 5*time.Millisecond, "Expected exactly 1 upload call")

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

// TestSpoolDriver_Close_NoFlush tests that Close does not flush when
// flush-on-close is disabled. Data should remain durable in the spool.
func TestSpoolDriver_Close_NoFlush(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	}))

	// when
	err := driver.Close()

	// then
	require.NoError(t, err)
	assert.Equal(t, 0, len(uploader.calls), "Close should not upload without WithFlushOnClose")

	// Verify spool files exist on disk for later recovery
	matches, _ := filepath.Glob(filepath.Join(spoolDir, "*.spool"))
	assert.NotEmpty(t, matches, "Spool files should remain on disk for recovery")
}

// TestSpoolDriver_Close_WithFlushOnClose tests that Close flushes when configured.
func TestSpoolDriver_Close_WithFlushOnClose(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "users", schema, []map[string]any{
		{"id": int64(1)},
	}))

	// when
	err := driver.Close()

	// then
	require.NoError(t, err)
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 1, callCount, "Close should upload with WithFlushOnClose(true)")
}

// TestSpoolDriver_Close_Idempotent tests that Close is safe to call multiple times.
func TestSpoolDriver_Close_Idempotent(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())

	// when - call Close twice
	err1 := driver.Close()
	err2 := driver.Close()

	// then - neither panics and both return nil
	require.NoError(t, err1)
	require.NoError(t, err2)
}

// TestSpoolDriver_RunFlush_UploadsMultipleTables tests that flush uploads
// segments for multiple tables.
func TestSpoolDriver_RunFlush_UploadsMultipleTables(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "users", schema,
		[]map[string]any{{"id": int64(1), "name": "Alice"}}))
	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(2), "name": "Bob"}}))

	// when
	err := driver.Close()

	// then
	require.NoError(t, err)
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 2, callCount, "Should upload 2 segments")
}

// TestSpoolDriver_RunFlush_ErrorHandling tests flush error handling.
func TestSpoolDriver_RunFlush_ErrorHandling(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, _, _ string) error {
			return fmt.Errorf("simulated upload error")
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "test", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when
	err := driver.runFlush(context.Background())

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated upload error")
}

// TestSpoolDriver_RunFlush_EmptyDirectory tests flush with no data.
func TestSpoolDriver_RunFlush_EmptyDirectory(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())
	t.Cleanup(func() { _ = driver.Close() })

	// when
	err := driver.runFlush(context.Background())

	// then
	require.NoError(t, err)
	assert.Equal(t, 0, len(uploader.calls))
}

// TestSpoolDriver_RepeatedUploadFailures_Quarantine tests that repeated
// upload failures eventually quarantine the spool file (stop retrying).
func TestSpoolDriver_RepeatedUploadFailures_Quarantine(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, _, _ string) error {
			return fmt.Errorf("persistent upload error")
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "users", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when - run flush 3 times (maxFailures=3)
	_ = driver.runFlush(context.Background())
	_ = driver.runFlush(context.Background())
	_ = driver.runFlush(context.Background())

	uploader.mu.Lock()
	callsAfter3 := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 3, callsAfter3)

	// when - run a 4th flush
	_ = driver.runFlush(context.Background())

	// then - uploader should NOT be called again after quarantine
	uploader.mu.Lock()
	callsAfter4 := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 3, callsAfter4, "No more uploads after quarantine")

	// Verify quarantine file exists
	matches, _ := filepath.Glob(filepath.Join(spoolDir, "*.quarantine"))
	assert.NotEmpty(t, matches, "Quarantined spool file should exist")
}

// TestSpoolDriver_RemoteKeyFormat tests that the remote key matches expected pattern.
func TestSpoolDriver_RemoteKeyFormat(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedRemoteKey string
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, _, remoteKey string) error {
			capturedRemoteKey = remoteKey
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "my_table", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when
	err := driver.Close()
	require.NoError(t, err)

	// then - verify remote key format
	// Pattern: table=<tableEsc>/schema=<fp>/y=<YYYY>/m=<MM>/d=<DD>/<unixSeconds>_<uuid>.csv
	pattern := `^table=my_table/schema=[a-f0-9]+/y=\d{4}/m=\d{2}/d=\d{2}/\d+_[a-f0-9\-]+\.csv$`
	matched, err := regexp.MatchString(pattern, capturedRemoteKey)
	require.NoError(t, err)
	assert.True(t, matched, "Remote key should match pattern, got: %s", capturedRemoteKey)
}

// TestSpoolDriver_FilesystemUploaderUsesNestedOutputPath tests filesystem uploads
// preserve the rendered path.
func TestSpoolDriver_FilesystemUploaderUsesNestedOutputPath(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	destDir := t.TempDir()
	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	driver := NewSpoolDriver(context.Background(), uploader, NewCSVFormat(), spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when
	err = driver.Close()

	// then
	require.NoError(t, err)
	fingerprint := schemaFingerprint(schema)
	matches, globErr := filepath.Glob(filepath.Join(
		destDir,
		"table=events",
		"schema="+fingerprint,
		"y=*",
		"m=*",
		"d=*",
		"*.csv",
	))
	require.NoError(t, globErr)
	require.Len(t, matches, 1)
	assert.FileExists(t, matches[0])
}

// TestCSVTimestamp_RFC3339Nano tests that timestamps are written with nanosecond precision.
func TestCSVTimestamp_RFC3339Nano(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedContent []byte
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, localPath, _ string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedContent = data
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ns},
	}, nil)

	ts := time.Date(2026, 2, 24, 14, 30, 45, 123456789, time.UTC)
	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"ts": ts}}))

	// when
	require.NoError(t, driver.Close())

	// then
	require.NotEmpty(t, capturedContent)
	reader := csv.NewReader(strings.NewReader(string(capturedContent)))
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, "ts", records[0][0])
	assert.Equal(t, "2026-02-24T14:30:45.123456789Z", records[1][0])
}

// TestSpoolDriver_Concurrency_WritesDuringFlush tests concurrent writes during flush cycles.
func TestSpoolDriver_Concurrency_WritesDuringFlush(t *testing.T) {
	// given
	spoolDir := t.TempDir()

	var capturedFiles [][]byte
	var capturedMu sync.Mutex

	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, localPath, _ string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedMu.Lock()
			capturedFiles = append(capturedFiles, data)
			capturedMu.Unlock()
			return nil
		},
	}
	format := NewCSVFormat()
	ctx := context.Background()

	driver := NewSpoolDriver(ctx, uploader, format, spoolDir,
		withManualCycle())

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
				_ = driver.runFlush(ctx)
				runtime.Gosched()
			}
		}
	}()

	// Wait for writers to finish
	writersWg.Wait()
	close(writersDone)
	flusherWg.Wait()

	// Drain remainder
	require.NoError(t, driver.runFlush(ctx))

	// then - count total data rows across all captured files
	capturedMu.Lock()
	files := capturedFiles
	capturedMu.Unlock()

	totalDataRows := 0
	for _, f := range files {
		reader := csv.NewReader(strings.NewReader(string(f)))
		records, err := reader.ReadAll()
		require.NoError(t, err)
		if len(records) > 1 {
			totalDataRows += len(records) - 1
		}
	}

	assert.Equal(t, numWriters*rowsPerWriter, totalDataRows,
		"Total data rows should equal numWriters * rowsPerWriter")
}

func TestParsePathTemplate(t *testing.T) {
	tests := []struct {
		name        string
		template    string
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid default template",
			template: "table={{.Table}}/schema={{.Schema}}/dt={{.Year}}/{{.MonthPadded}}/{{.DayPadded}}/" +
				"{{.SegmentID}}.{{.Extension}}",
			expectError: false,
		},
		{
			name:        "valid simple template",
			template:    "{{.Table}}/{{.SegmentID}}.{{.Extension}}",
			expectError: false,
		},
		{
			name: "valid template with all variables",
			template: "{{.Table}}_{{.Schema}}_{{.Year}}_{{.Month}}_{{.MonthPadded}}_{{.Day}}_" +
				"{{.DayPadded}}_{{.SegmentID}}.{{.Extension}}",
			expectError: false,
		},
		{
			name:        "empty string",
			template:    "",
			expectError: true,
			errorMsg:    "template string cannot be empty",
		},
		{
			name:        "whitespace only",
			template:    "   ",
			expectError: true,
			errorMsg:    "template string cannot be empty",
		},
		{
			name:        "invalid template syntax",
			template:    "{{.Table",
			expectError: true,
			errorMsg:    "parsing template",
		},
		{
			name:        "template with path traversal in literal",
			template:    "../{{.Table}}/{{.SegmentID}}.{{.Extension}}",
			expectError: true,
			errorMsg:    "template output contains path traversal sequence (..)",
		},
		{
			name:        "template with path traversal in middle",
			template:    "{{.Table}}/../{{.SegmentID}}.{{.Extension}}",
			expectError: true,
			errorMsg:    "template output contains path traversal sequence (..)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			tmpl, err := parsePathTemplate(tt.template)

			// then
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, tmpl)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, tmpl)
			}
		})
	}
}

func TestSpoolDriver_WithPathTemplate(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	ctx := context.Background()
	up := &mockUploader{}
	customTemplate := "custom/{{.Table}}/{{.SegmentID}}.{{.Extension}}"

	// when
	driver := NewSpoolDriver(ctx, up, NewCSVFormat(), spoolDir,
		WithPathTemplate(customTemplate),
		withManualCycle())

	// then
	assert.NotNil(t, driver.pathTemplate)
	assert.Equal(t, customTemplate, driver.pathTemplateStr)

	_ = driver.Close()
}

func TestSpoolDriver_DefaultPathTemplate(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	ctx := context.Background()
	up := &mockUploader{}

	// when
	driver := NewSpoolDriver(ctx, up, NewCSVFormat(), spoolDir,
		withManualCycle())

	// then
	assert.NotNil(t, driver.pathTemplate)
	assert.Empty(t, driver.pathTemplateStr)

	_ = driver.Close()
}

func TestSpoolDriver_InvalidPathTemplate_Panics(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	ctx := context.Background()
	up := &mockUploader{}

	// when/then
	assert.Panics(t, func() {
		NewSpoolDriver(ctx, up, NewCSVFormat(), spoolDir,
			WithPathTemplate("{{.Table"),
			withManualCycle())
	})
}

func TestSpoolDriver_PathTraversalTemplate_Panics(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	ctx := context.Background()
	up := &mockUploader{}

	// when/then
	assert.Panics(t, func() {
		NewSpoolDriver(ctx, up, NewCSVFormat(), spoolDir,
			WithPathTemplate("../{{.Table}}/{{.SegmentID}}.{{.Extension}}"),
			withManualCycle())
	})
}

// TestSpoolDriver_Write_ErrorHandling tests error cases.
func TestSpoolDriver_Write_ErrorHandling(t *testing.T) {
	// given - use a non-writable spool directory
	up := &mockUploader{}
	format := NewCSVFormat()

	// Attempting to use /nonexistent should fail at spool creation — panics.
	assert.Panics(t, func() {
		NewSpoolDriver(context.Background(), up, format, "/nonexistent/invalid/path",
			withManualCycle())
	})
}

// TestSpoolDriver_Recovery tests that data written then left (no flush) can
// be recovered by a new driver instance.
func TestSpoolDriver_Recovery(t *testing.T) {
	// given - write data, close without flushing
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()

	driver1 := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver1.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(42)}}))

	require.NoError(t, driver1.Close())
	assert.Empty(t, uploader.calls, "Should not have uploaded")

	// when - create a new driver pointing at the same spool dir, with flush-on-close
	var capturedCSV []byte
	uploader2 := &mockUploader{
		uploadFunc: func(_ context.Context, localPath, _ string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedCSV = data
			return nil
		},
	}
	driver2 := NewSpoolDriver(context.Background(), uploader2, format, spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))

	require.NoError(t, driver2.Close())

	// then - the recovered data should have been flushed
	uploader2.mu.Lock()
	callCount := len(uploader2.calls)
	uploader2.mu.Unlock()
	assert.Equal(t, 1, callCount, "Recovered data should be uploaded")

	require.NotEmpty(t, capturedCSV)
	reader := csv.NewReader(strings.NewReader(string(capturedCSV)))
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, "id", records[0][0])
	assert.Equal(t, "42", records[1][0])
}

// TestSpoolDriver_ForceAll_OnClose tests that Close forces flush of all data
// regardless of size thresholds.
func TestSpoolDriver_ForceAll_OnClose(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithMaxSegmentSize(1<<30), // Large threshold
		WithFlushOnClose(true))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write small amount of data (below threshold)
	require.NoError(t, driver.Write(context.Background(), "users", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when
	err := driver.Close()

	// then
	require.NoError(t, err)
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 1, callCount, "Close should force flush and upload")
}

// TestSpoolDriver_Close_Lifecycle tests Close behavior with timer.
func TestSpoolDriver_Close_Lifecycle(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		WithSealCheckInterval(1*time.Second),
		WithFlushOnClose(true))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "users", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when
	err := driver.Close()

	// then
	require.NoError(t, err)
	assert.Equal(t, 1, len(uploader.calls), "Close should upload active segment")
}

// TestSpoolDriver_MultipleBatches_SingleUpload tests that multiple write batches
// to the same table+schema are combined into a single upload on flush.
func TestSpoolDriver_MultipleBatches_SingleUpload(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	var capturedCSV []byte
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, localPath, _ string) error {
			data, err := os.ReadFile(localPath) //nolint:gosec // test code
			if err != nil {
				return err
			}
			capturedCSV = data
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle(),
		WithFlushOnClose(true))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	// when - write two batches then flush
	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(1), "value": "first"}}))
	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(2), "value": "second"}}))

	err := driver.Close()
	require.NoError(t, err)

	// then - both batches in a single upload
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 1, callCount, "Both batches should be in one upload")

	require.NotEmpty(t, capturedCSV)
	reader := csv.NewReader(strings.NewReader(string(capturedCSV)))
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 3) // header + 2 rows
	assert.Equal(t, []string{"id", "value"}, records[0])
	assert.Equal(t, "1", records[1][0])
	assert.Equal(t, "first", records[1][1])
	assert.Equal(t, "2", records[2][0])
	assert.Equal(t, "second", records[2][1])
}

// TestSpoolDriver_RetryUsesStableRemoteKey verifies that when an upload fails
// and is retried, the same remote key is used (no duplicates from regenerated
// segment IDs).
func TestSpoolDriver_RetryUsesStableRemoteKey(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	callCount := 0
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, _, _ string) error {
			callCount++
			if callCount <= 2 {
				return fmt.Errorf("transient failure")
			}
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when — flush fails twice, then succeeds on the third attempt
	_ = driver.runFlush(context.Background())
	_ = driver.runFlush(context.Background())
	require.NoError(t, driver.runFlush(context.Background()))

	// then — all three attempts used the same remote key
	uploader.mu.Lock()
	calls := uploader.calls
	uploader.mu.Unlock()
	require.Len(t, calls, 3)
	assert.Equal(t, calls[0].remoteKey, calls[1].remoteKey,
		"retry #1 and #2 should use same remote key")
	assert.Equal(t, calls[1].remoteKey, calls[2].remoteKey,
		"retry #2 and #3 should use same remote key")
}

// TestSpoolDriver_RetryStableRemoteKey_MultipleBatches verifies stable
// identity when multiple Write calls land in the same active file.
func TestSpoolDriver_RetryStableRemoteKey_MultipleBatches(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	callCount := 0
	uploader := &mockUploader{
		uploadFunc: func(_ context.Context, _, _ string) error {
			callCount++
			if callCount == 1 {
				return fmt.Errorf("transient failure")
			}
			return nil
		},
	}
	format := NewCSVFormat()
	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		withManualCycle())
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write two batches before any flush
	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(1)}}))
	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(2)}}))

	// when — first flush fails, second succeeds
	_ = driver.runFlush(context.Background())
	require.NoError(t, driver.runFlush(context.Background()))

	// then — both attempts used the same remote key (from first frame)
	uploader.mu.Lock()
	calls := uploader.calls
	uploader.mu.Unlock()
	require.Len(t, calls, 2)
	assert.Equal(t, calls[0].remoteKey, calls[1].remoteKey,
		"retry should use same remote key as first attempt")
}

// TestSpoolDriver_MaxSegmentAge_BlocksYoungData verifies that when
// maxSegmentAge is set, the timer-based flush does NOT upload data that is
// younger than the threshold.
func TestSpoolDriver_MaxSegmentAge_BlocksYoungData(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	mt := newManualTicker()

	now := time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		WithSealCheckInterval(time.Second),
		WithMaxSegmentAge(10*time.Minute),
		withManualTicker(mt),
		withNowFunc(clock),
	)
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write data — written at now (12:00:00)
	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when — tick fires, but only 1 minute has elapsed (< 10 min threshold)
	now = now.Add(1 * time.Minute)
	mt.tick()

	// Give the timer goroutine a moment to process
	time.Sleep(50 * time.Millisecond)

	// then — no upload because data is too young
	uploader.mu.Lock()
	callCount := len(uploader.calls)
	uploader.mu.Unlock()
	assert.Equal(t, 0, callCount, "Young data should not be flushed")
}

// TestSpoolDriver_MaxSegmentAge_FlushesOldData verifies that when
// maxSegmentAge is set, the timer-based flush uploads data once it exceeds
// the age threshold.
func TestSpoolDriver_MaxSegmentAge_FlushesOldData(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	mt := newManualTicker()

	now := time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		WithSealCheckInterval(time.Second),
		WithMaxSegmentAge(10*time.Minute),
		withManualTicker(mt),
		withNowFunc(clock),
	)
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Write data at 12:00:00
	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when — advance time past threshold and tick
	mu.Lock()
	now = now.Add(11 * time.Minute)
	mu.Unlock()
	mt.tick()

	// then — data is old enough, should be uploaded
	require.Eventually(t, func() bool {
		uploader.mu.Lock()
		defer uploader.mu.Unlock()
		return len(uploader.calls) == 1
	}, 2*time.Second, 5*time.Millisecond, "Old data should be flushed")
}

// TestSpoolDriver_MaxSegmentAge_ZeroAlwaysFlushes verifies that when
// maxSegmentAge is 0, every timer tick triggers a flush (backwards
// compatible behavior).
func TestSpoolDriver_MaxSegmentAge_ZeroAlwaysFlushes(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	uploader := &mockUploader{}
	format := NewCSVFormat()
	mt := newManualTicker()

	driver := NewSpoolDriver(context.Background(), uploader, format, spoolDir,
		WithSealCheckInterval(time.Second),
		// maxSegmentAge deliberately not set (defaults to 0)
		withManualTicker(mt),
	)
	t.Cleanup(func() { _ = driver.Close() })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	require.NoError(t, driver.Write(context.Background(), "events", schema,
		[]map[string]any{{"id": int64(1)}}))

	// when — tick fires immediately
	mt.tick()

	// then — should be uploaded despite data being very fresh
	require.Eventually(t, func() bool {
		uploader.mu.Lock()
		defer uploader.mu.Unlock()
		return len(uploader.calls) == 1
	}, 2*time.Second, 5*time.Millisecond, "With maxSegmentAge=0, every tick should flush")
}
