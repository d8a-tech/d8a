package files

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/sirupsen/logrus"
)

// SpoolOption is a functional option for configuring SpoolDriver.
type SpoolOption func(*spoolDriver)

// WithManualFlush disables automatic timer-based flushing.
func WithManualFlush() SpoolOption {
	return func(sd *spoolDriver) {
		sd.flushInterval = 0
	}
}

// spoolDriver is a warehouse.Driver that writes analytics data directly to disk
// files and periodically uploads them to object storage.
//
// Disk is the source of truth - no in-memory state is maintained.
// Each Write() appends rows directly to disk; a timer goroutine periodically
// scans the spool directory and uploads files.
type spoolDriver struct {
	ctx           context.Context
	uploader      Uploader
	format        Format
	spoolDir      string
	flushInterval time.Duration
	stopCh        chan struct{}  // signal to stop timer
	wg            sync.WaitGroup // wait for timer goroutine
	mu            sync.Mutex     // protects concurrent file operations
}

// NewSpoolDriver creates a new spool driver that writes rows directly to disk
// and periodically uploads files.
func NewSpoolDriver(
	ctx context.Context,
	uploader Uploader,
	format Format,
	spoolDir string,
	flushInterval time.Duration,
	opts ...SpoolOption,
) *spoolDriver {
	sd := &spoolDriver{
		ctx:           ctx,
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
		return
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
				if err := sd.Flush(sd.ctx); err != nil {
					logrus.WithError(err).Error("automatic flush failed")
				}
			case <-sd.ctx.Done():
				return
			case <-sd.stopCh:
				return
			}
		}
	}()
}

// Write appends rows to disk file immediately and creates metadata.
// All file operations are mutex-protected to prevent concurrent writes.
func (sd *spoolDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	fingerprint := SchemaFingerprint(schema)
	csvFile := FilenameForWrite(table, fingerprint, sd.format)
	metaFile := MetadataFilename(table, fingerprint)
	csvPath := filepath.Join(sd.spoolDir, csvFile)
	metaPath := filepath.Join(sd.spoolDir, metaFile)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	// Check if file exists to determine open flags
	_, statErr := os.Stat(csvPath)
	fileExists := !os.IsNotExist(statErr)

	var openFlags int
	if fileExists {
		openFlags = os.O_APPEND | os.O_WRONLY
	} else {
		openFlags = os.O_CREATE | os.O_WRONLY
	}

	//nolint:gosec // G304: csvPath from controlled inputs
	file, err := os.OpenFile(csvPath, openFlags, 0o600)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"table":       table,
			"fingerprint": fingerprint,
			"path":        csvPath,
		}).Error("failed to open CSV file")
		return fmt.Errorf("opening CSV file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			logrus.WithError(closeErr).Error("failed to close CSV file")
		}
	}()

	if err := sd.format.Write(file, schema, rows); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"table":       table,
			"fingerprint": fingerprint,
			"row_count":   len(rows),
		}).Error("failed to write rows to file")
		return fmt.Errorf("writing rows to CSV file: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"table":       table,
		"fingerprint": fingerprint,
		"row_count":   len(rows),
	}).Debug("wrote rows to file")

	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		if err := sd.createMetadataFile(metaPath, table, fingerprint, schema); err != nil {
			return err
		}
	}

	return nil
}

// createMetadataFile creates a metadata file atomically for a CSV file.
// Atomic write (tmp + rename) prevents flush timer from reading incomplete metadata.
func (sd *spoolDriver) createMetadataFile(metaPath, table, fingerprint string, schema *arrow.Schema) error {
	encodedSchema, err := SerializeSchema(schema)
	if err != nil {
		logrus.WithError(err).WithField("table", table).Error("failed to serialize schema")
		return fmt.Errorf("serializing schema: %w", err)
	}

	meta := &Metadata{
		Table:       table,
		Fingerprint: fingerprint,
		Schema:      encodedSchema,
		CreatedAt:   time.Now().Format(time.RFC3339),
	}

	tmpPath := metaPath + ".tmp"
	tmpFile, err := os.Create(tmpPath) //nolint:gosec // tmpPath is constructed from controlled inputs
	if err != nil {
		logrus.WithError(err).WithField("path", tmpPath).Error("failed to create temp metadata file")
		return fmt.Errorf("creating temp metadata file: %w", err)
	}

	if err := WriteMetadata(tmpFile, meta); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		logrus.WithError(err).WithField("table", table).Error("failed to write metadata")
		return fmt.Errorf("writing metadata: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		logrus.WithError(err).WithField("path", tmpPath).Error("failed to close temp metadata file")
		return fmt.Errorf("closing temp metadata file: %w", err)
	}

	if err := os.Rename(tmpPath, metaPath); err != nil {
		_ = os.Remove(tmpPath)
		logrus.WithError(err).WithFields(logrus.Fields{
			"tmp":  tmpPath,
			"meta": metaPath,
		}).Error("failed to rename metadata file")
		return fmt.Errorf("renaming metadata file: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"table":       table,
		"fingerprint": fingerprint,
	}).Debug("created metadata file")

	return nil
}

// CreateTable is a no-op for spool drivers.
func (sd *spoolDriver) CreateTable(table string, schema *arrow.Schema) error {
	return nil
}

// AddColumn is a no-op for spool drivers.
func (sd *spoolDriver) AddColumn(table string, field *arrow.Field) error {
	return nil
}

// MissingColumns always returns an empty slice for spool drivers.
func (sd *spoolDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	return []*arrow.Field{}, nil
}

// Flush scans the spool directory for CSV files and uploads them.
func (sd *spoolDriver) Flush(ctx context.Context) error {
	csvFiles, err := FindCSVFiles(sd.spoolDir)
	if err != nil {
		logrus.WithError(err).Error("failed to scan spool directory")
		return fmt.Errorf("scanning spool directory: %w", err)
	}

	logrus.WithField("file_count", len(csvFiles)).Info("starting flush")

	if len(csvFiles) == 0 {
		logrus.Debug("no files to upload")
		return nil
	}

	var errs []error
	successCount := 0

	for _, csvPath := range csvFiles {
		metaPath := GetMetadataPathForCSV(csvPath)

		if _, err := os.Stat(metaPath); os.IsNotExist(err) {
			logrus.WithFields(logrus.Fields{
				"csv_path":  csvPath,
				"meta_path": metaPath,
			}).Warn("CSV file found without metadata, skipping")
			continue
		}

		meta, err := LoadMetadataFile(metaPath)
		if err != nil {
			logrus.WithError(err).WithField("meta_path", metaPath).Warn("failed to read metadata, skipping file")
			continue
		}

		if err := sd.uploader.Upload(ctx, csvPath); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"csv_path":    csvPath,
				"table":       meta.Table,
				"fingerprint": meta.Fingerprint,
			}).Error("failed to upload file")
			errs = append(errs, fmt.Errorf("uploading %s: %w", csvPath, err))
			continue
		}

		if err := DeleteMetadataFile(metaPath); err != nil {
			logrus.WithError(err).WithField("meta_path", metaPath).Warn("failed to delete metadata file after successful upload")
		}

		logrus.WithFields(logrus.Fields{
			"table":       meta.Table,
			"fingerprint": meta.Fingerprint,
		}).Info("uploaded file")
		successCount++
	}

	logrus.WithFields(logrus.Fields{
		"success_count": successCount,
		"error_count":   len(errs),
	}).Info("flush completed")

	if len(errs) > 0 {
		var combined error
		for _, err := range errs {
			if combined == nil {
				combined = err
			} else {
				combined = fmt.Errorf("%w; %w", combined, err)
			}
		}
		return combined
	}

	return nil
}

// Close gracefully shuts down the driver.
func (sd *spoolDriver) Close() error {
	close(sd.stopCh)
	sd.wg.Wait()
	err := sd.Flush(context.Background())
	logrus.Info("driver closed")
	return err
}
