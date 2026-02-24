package files

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/sirupsen/logrus"
)

// SpoolOption is a functional option for configuring SpoolDriver.
type SpoolOption func(*spoolDriver)

// WithManualFlush disables automatic timer-based flushing.
// Useful for testing or when you want to call Flush() manually.
func WithManualFlush() SpoolOption {
	return func(sd *spoolDriver) {
		sd.flushInterval = 0
	}
}

// spoolDriver is a warehouse.Driver that writes analytics data directly to disk
// files and periodically uploads them to object storage.
//
// The driver maintains NO in-memory state - disk is the source of truth.
// Each Write() call appends rows directly to disk files. A timer goroutine
// periodically scans the spool directory and uploads CSV files.
type spoolDriver struct {
	uploader      Uploader
	format        Format
	spoolDir      string
	flushInterval time.Duration
	stopCh        chan struct{}  // signal to stop timer
	wg            sync.WaitGroup // wait for timer goroutine
	mu            sync.Mutex     //nolint:unused // used in Write and Flush (Tasks 5-6)
}

// NewSpoolDriver creates a new spool driver that writes rows directly to disk
// and periodically uploads files.
//
// The driver writes each Write() call synchronously to disk files without
// in-memory buffering. A timer goroutine fires periodically to scan the spool
// directory and upload CSV files. Files are uploaded via the provided Uploader.
//
// Parameters:
//   - uploader: handles moving files to final destination (cloud/local)
//   - format: serialization format (CSV, Parquet, etc.)
//   - spoolDir: directory for temporary spool files (must exist)
//   - flushInterval: time between automatic flushes (0 = manual only)
//   - opts: optional configuration
func NewSpoolDriver(
	uploader Uploader,
	format Format,
	spoolDir string,
	flushInterval time.Duration,
	opts ...SpoolOption,
) *spoolDriver {
	sd := &spoolDriver{
		uploader:      uploader,
		format:        format,
		spoolDir:      spoolDir,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}

	for _, opt := range opts {
		opt(sd)
	}

	sd.startTimer()

	return sd
}

// startTimer starts the periodic flush timer (if interval > 0).
func (sd *spoolDriver) startTimer() {
	if sd.flushInterval == 0 {
		return // manual flush mode
	}

	sd.wg.Add(1)
	go func() {
		defer sd.wg.Done()
		logrus.WithField("interval", sd.flushInterval).Info("started flush timer")

		ticker := time.NewTicker(sd.flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := sd.Flush(context.Background()); err != nil {
					logrus.WithError(err).Error("automatic flush failed")
				}
			case <-sd.stopCh:
				return
			}
		}
	}()
}

// Write appends rows to disk file immediately and creates metadata.
//
// This method:
//  1. Calculates CSV filename based on schema fingerprint and table name
//  2. Opens CSV file in append mode (creates if missing)
//  3. Writes rows using the configured Format
//  4. Creates/updates metadata file with schema and table info
//  5. Returns immediately (no buffering)
//
// All file operations are protected by a mutex to prevent concurrent writes
// to the same file.
//
// Error handling:
//   - File write errors: logged and returned, file left on disk for retry
//   - Metadata write errors: logged and returned
func (sd *spoolDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	// Placeholder - will be implemented in Task 5
	logrus.WithFields(logrus.Fields{
		"table":     table,
		"row_count": len(rows),
		"fields":    len(schema.Fields()),
	}).Info("spool driver stub: would write rows to disk")

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

// Flush scans the spool directory for CSV files and uploads them.
//
// This method:
//  1. Scans the spool directory for CSV files
//  2. For each CSV file, uploads it via the Uploader
//  3. Deletes the CSV and metadata files after successful upload
//  4. Returns combined error if any uploads failed
//
// This is useful for graceful shutdown or manual trigger points.
func (sd *spoolDriver) Flush(ctx context.Context) error {
	// Placeholder - will be implemented in Task 6
	logrus.Info("spool driver stub: would scan and upload CSV files")
	return nil
}

// Close gracefully shuts down the driver.
//
// This method:
//  1. Closes the stop channel to signal the timer goroutine to exit
//  2. Waits for the timer goroutine to complete
//  3. Performs a final Flush to upload remaining files
//  4. Returns any errors from the final flush
func (sd *spoolDriver) Close() error {
	close(sd.stopCh)
	sd.wg.Wait()
	err := sd.Flush(context.Background())
	logrus.Info("driver closed")
	return err
}
