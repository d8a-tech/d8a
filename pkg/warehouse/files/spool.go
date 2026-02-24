package files

import (
	"context"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/sirupsen/logrus"
)

// FlushTrigger determines when buffered data should be flushed to disk.
type FlushTrigger interface {
	// ShouldFlush returns true if the buffer should be flushed based on
	// the current row count and time since the buffer was created.
	ShouldFlush(rowCount int, age time.Duration) bool
}

// SpoolOption is a functional option for configuring SpoolDriver.
type SpoolOption func(*spoolDriver)

// WithFlushTrigger sets the flush trigger for the spool driver.
func WithFlushTrigger(trigger FlushTrigger) SpoolOption {
	return func(sd *spoolDriver) {
		sd.flushTrigger = trigger
	}
}

// tableBuffer holds in-memory state for a single table's data.
type tableBuffer struct {
	schema    *arrow.Schema
	rows      []map[string]any
	createdAt time.Time
}

// spoolDriver is a warehouse.Driver decorator that buffers writes to local files
// before uploading them to a final destination.
//
// It maintains per-table buffers in memory, writes them to temporary files when
// flush triggers fire, then uploads those files via the Uploader interface.
type spoolDriver struct {
	uploader     Uploader
	format       Format
	spoolDir     string
	flushTrigger FlushTrigger
	buffers      map[string]*tableBuffer
}

// NewSpoolDriver creates a new spool driver that buffers writes and uploads files.
//
// The driver maintains in-memory buffers per table and flushes them to local files
// in spoolDir when the flush trigger condition is met. Files are then uploaded
// via the provided Uploader.
//
// Parameters:
//   - uploader: handles moving files to final destination (cloud/local)
//   - format: serialization format (CSV, Parquet, etc.)
//   - spoolDir: directory for temporary spool files (must exist)
//   - opts: optional configuration (flush trigger, etc.)
func NewSpoolDriver(uploader Uploader, format Format, spoolDir string, opts ...SpoolOption) *spoolDriver {
	sd := &spoolDriver{
		uploader:     uploader,
		format:       format,
		spoolDir:     spoolDir,
		flushTrigger: nil, // Default: no auto-flush
		buffers:      make(map[string]*tableBuffer),
	}

	for _, opt := range opts {
		opt(sd)
	}

	return sd
}

// Write buffers rows for a table and flushes to disk when triggered.
//
// Production flow (not yet implemented):
//
//  1. Schema change detection:
//     - Check if we have a buffer for this table
//     - If buffer exists and schema differs from incoming schema:
//     * Flush existing buffer immediately (schema changed)
//     * Remove old buffer from map
//
//  2. Buffer creation/management:
//     - If no buffer exists for table:
//     * Create new tableBuffer with schema, empty rows slice, current timestamp
//     * Store in sd.buffers[table]
//     - Append incoming rows to buffer.rows
//
//  3. Flush trigger check:
//     - If flushTrigger is nil, skip auto-flush (manual flush only)
//     - Otherwise calculate buffer age: time.Since(buffer.createdAt)
//     - Call flushTrigger.ShouldFlush(len(buffer.rows), age)
//     - If false, return early (no flush needed)
//
//  4. File naming and writing:
//     - Generate filename: fmt.Sprintf("%s_%d.%s.tmp", table, time.Now().Unix(), sd.format.Extension())
//     * Example: "events_1234567890.parquet.tmp"
//     * The .tmp suffix prevents partial files from being processed
//     - Full path: filepath.Join(sd.spoolDir, filename)
//     - Create file: os.Create(fullPath)
//     - Write buffer data: sd.format.Write(file, buffer.schema, buffer.rows)
//     - Close file
//
//  5. Upload and cleanup:
//     - Remove .tmp suffix: finalPath = strings.TrimSuffix(fullPath, ".tmp")
//     - Atomically rename: os.Rename(fullPath, finalPath)
//     * This makes the file visible for upload only after complete write
//     - Upload file: sd.uploader.Upload(ctx, finalPath)
//     - Delete local file after successful upload: os.Remove(finalPath)
//     - Delete buffer from map: delete(sd.buffers, table)
//
// Error handling:
//   - File write errors: return immediately, preserve buffer for retry
//   - Upload errors: return immediately, leave file on disk for manual recovery
//   - Schema detection errors: flush existing buffer before returning error
func (sd *spoolDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	logrus.WithFields(logrus.Fields{
		"table":     table,
		"row_count": len(rows),
		"fields":    len(schema.Fields()),
	}).Info("spool driver stub: would buffer and conditionally flush rows")

	return nil
}

// CreateTable is a no-op for spool drivers.
// Schema management is handled by the underlying destination system.
func (sd *spoolDriver) CreateTable(table string, schema *arrow.Schema) error {
	return nil
}

// AddColumn is a no-op for spool drivers.
// Schema evolution is handled by the underlying destination system.
func (sd *spoolDriver) AddColumn(table string, field *arrow.Field) error {
	return nil
}

// MissingColumns always returns an empty slice for spool drivers.
// Schema validation is deferred to the destination system.
func (sd *spoolDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	return []*arrow.Field{}, nil
}

// Flush forces all buffered data to be written and uploaded immediately.
// This is useful for graceful shutdown or manual trigger points.
//
// Production implementation would:
//   - Iterate through all buffers in sd.buffers
//   - For each buffer, execute the flush flow from Write() steps 4-5
//   - Collect and return any errors encountered
//   - Clear sd.buffers map after successful flush
func (sd *spoolDriver) Flush(ctx context.Context) error {
	tableCount := len(sd.buffers)
	if tableCount == 0 {
		return nil
	}

	logrus.WithField("table_count", tableCount).Info("spool driver stub: would flush all buffers")
	return nil
}

// BufferStats returns current buffer statistics for observability.
// Used for monitoring buffer sizes and ages.
type BufferStats struct {
	Table     string
	RowCount  int
	Age       time.Duration
	FieldsLen int
}

// Stats returns current statistics for all buffered tables.
// Useful for monitoring, debugging, and triggering manual flushes.
func (sd *spoolDriver) Stats() []BufferStats {
	stats := make([]BufferStats, 0, len(sd.buffers))
	now := time.Now()

	for table, buffer := range sd.buffers {
		stats = append(stats, BufferStats{
			Table:     table,
			RowCount:  len(buffer.rows),
			Age:       now.Sub(buffer.createdAt),
			FieldsLen: len(buffer.schema.Fields()),
		})
	}

	return stats
}

// defaultFlushTrigger is a simple trigger based on row count and time thresholds.
type defaultFlushTrigger struct {
	maxRows int
	maxAge  time.Duration
}

// NewDefaultFlushTrigger creates a flush trigger that fires when either
// row count or age threshold is exceeded.
func NewDefaultFlushTrigger(maxRows int, maxAge time.Duration) FlushTrigger {
	return &defaultFlushTrigger{
		maxRows: maxRows,
		maxAge:  maxAge,
	}
}

// ShouldFlush implements FlushTrigger.
func (t *defaultFlushTrigger) ShouldFlush(rowCount int, age time.Duration) bool {
	return rowCount >= t.maxRows || age >= t.maxAge
}

// neverFlushTrigger is a trigger that never fires automatically.
// Useful for manual flush control or testing.
type neverFlushTrigger struct{}

// NewNeverFlushTrigger creates a trigger that never fires automatically.
func NewNeverFlushTrigger() FlushTrigger {
	return &neverFlushTrigger{}
}

// ShouldFlush implements FlushTrigger.
func (t *neverFlushTrigger) ShouldFlush(rowCount int, age time.Duration) bool {
	return false
}
