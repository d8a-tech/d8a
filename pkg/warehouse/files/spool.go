package files

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
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
	streams       map[string]*streamState
}

type streamState struct {
	createdAt       time.Time
	activeSizeBytes int64
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
		streams:       make(map[string]*streamState),
	}

	for _, opt := range opts {
		opt(sd)
	}

	if err := sd.recoverStreams(); err != nil {
		logrus.WithError(err).Error("failed to recover streams")
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
	tableEsc := EscapeTableName(table)
	csvPath := ActivePath(sd.spoolDir, tableEsc, fingerprint)
	metaPath := filepath.Join(StreamDir(sd.spoolDir, tableEsc, fingerprint), "active.meta.json")
	key := streamKey(tableEsc, fingerprint)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	if err := EnsureStreamDirs(sd.spoolDir, tableEsc, fingerprint); err != nil {
		return fmt.Errorf("ensuring stream directories: %w", err)
	}

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

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("statting active CSV file: %w", err)
	}

	state, exists := sd.streams[key]
	if !exists {
		state = &streamState{createdAt: time.Now().UTC()}
		sd.streams[key] = state
	}
	state.activeSizeBytes = fileInfo.Size()

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

// createMetadataFile creates the active.meta.json sidecar for a stream.
// Delegates to SaveMetadataFile for atomic tmp+rename write.
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
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	if err := SaveMetadataFile(metaPath, meta); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"table":       table,
			"fingerprint": fingerprint,
		}).Error("failed to create metadata file")
		return fmt.Errorf("creating metadata file: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"table":       table,
		"fingerprint": fingerprint,
	}).Debug("created metadata file")

	return nil
}

func streamKey(tableEsc, fingerprint string) string {
	return fmt.Sprintf("%s/%s", tableEsc, fingerprint)
}

func (sd *spoolDriver) sealStream(tableEsc, fingerprint string) (segmentID string, sealTime time.Time, err error) {
	segmentID = uuid.NewString()
	sealTime = time.Now().UTC()

	activePath := ActivePath(sd.spoolDir, tableEsc, fingerprint)
	sealedDir := SealedDir(sd.spoolDir, tableEsc, fingerprint)
	sealedPath := SegmentPath(sealedDir, segmentID)
	if err := os.Rename(activePath, sealedPath); err != nil {
		return "", time.Time{}, fmt.Errorf("sealing active segment: %w", err)
	}

	activeMetaPath := filepath.Join(StreamDir(sd.spoolDir, tableEsc, fingerprint), "active.meta.json")
	sealedMetaPath := filepath.Join(sealedDir, fmt.Sprintf("%s.meta.json", segmentID))
	if _, err := os.Stat(activeMetaPath); err == nil {
		if err := os.Rename(activeMetaPath, sealedMetaPath); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"table":       tableEsc,
				"fingerprint": fingerprint,
				"segment_id":  segmentID,
			}).Warn("failed to move metadata file during seal")
		}
	} else if !os.IsNotExist(err) {
		logrus.WithError(err).WithFields(logrus.Fields{
			"table":       tableEsc,
			"fingerprint": fingerprint,
			"segment_id":  segmentID,
		}).Warn("failed to stat metadata file during seal")
	}

	delete(sd.streams, streamKey(tableEsc, fingerprint))

	return segmentID, sealTime, nil
}

func (sd *spoolDriver) recoverStreams() error {
	streamsRoot := filepath.Join(sd.spoolDir, "streams")
	entries, err := os.ReadDir(streamsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading streams root: %w", err)
	}

	for _, tableEntry := range entries {
		if !tableEntry.IsDir() {
			continue
		}

		tableEsc := tableEntry.Name()
		tableDir := filepath.Join(streamsRoot, tableEsc)
		fpEntries, err := os.ReadDir(tableDir)
		if err != nil {
			return fmt.Errorf("reading table directory %s: %w", tableDir, err)
		}

		for _, fpEntry := range fpEntries {
			if !fpEntry.IsDir() {
				continue
			}

			fingerprint := fpEntry.Name()
			streamDir := filepath.Join(tableDir, fingerprint)

			if err := cleanTempFiles(streamDir); err != nil {
				return fmt.Errorf("cleaning temp files for %s: %w", streamDir, err)
			}

			if err := sd.recoverUploading(tableEsc, fingerprint); err != nil {
				return fmt.Errorf("recovering uploading segments for %s: %w", streamDir, err)
			}

			activePath := ActivePath(sd.spoolDir, tableEsc, fingerprint)
			activeInfo, err := os.Stat(activePath)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return fmt.Errorf("statting active CSV %s: %w", activePath, err)
			}

			metaPath := filepath.Join(streamDir, "active.meta.json")
			var createdAt time.Time
			if _, err := os.Stat(metaPath); err == nil {
				meta, err := LoadMetadataFile(metaPath)
				if err != nil {
					return fmt.Errorf("loading metadata %s: %w", metaPath, err)
				}
				createdAt, err = time.Parse(time.RFC3339, meta.CreatedAt)
				if err != nil {
					return fmt.Errorf("parsing metadata created_at: %w", err)
				}
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("statting metadata file %s: %w", metaPath, err)
			} else {
				createdAt = activeInfo.ModTime()
			}

			key := streamKey(tableEsc, fingerprint)
			sd.streams[key] = &streamState{
				createdAt:       createdAt,
				activeSizeBytes: activeInfo.Size(),
			}
		}
	}

	return nil
}

func (sd *spoolDriver) recoverUploading(tableEsc, fingerprint string) error {
	uploadingDir := UploadingDir(sd.spoolDir, tableEsc, fingerprint)
	entries, err := os.ReadDir(uploadingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading uploading dir %s: %w", uploadingDir, err)
	}

	sealedDir := SealedDir(sd.spoolDir, tableEsc, fingerprint)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) != ".csv" {
			continue
		}

		segmentID := name[:len(name)-len(".csv")]
		uploadingPath := filepath.Join(uploadingDir, name)
		sealedPath := filepath.Join(sealedDir, name)
		if err := os.Rename(uploadingPath, sealedPath); err != nil {
			return fmt.Errorf("moving uploading segment %s: %w", uploadingPath, err)
		}

		uploadingMeta := filepath.Join(uploadingDir, fmt.Sprintf("%s.meta.json", segmentID))
		sealedMeta := filepath.Join(sealedDir, fmt.Sprintf("%s.meta.json", segmentID))
		if _, err := os.Stat(uploadingMeta); err == nil {
			if err := os.Rename(uploadingMeta, sealedMeta); err != nil {
				return fmt.Errorf("moving uploading metadata %s: %w", uploadingMeta, err)
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("statting uploading metadata %s: %w", uploadingMeta, err)
		}

		logrus.WithFields(logrus.Fields{
			"table":       tableEsc,
			"fingerprint": fingerprint,
			"segment_id":  segmentID,
		}).Info("recovered uploading segment")
	}

	return nil
}

func cleanTempFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading dir %s: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != ".tmp" {
			continue
		}
		path := filepath.Join(dir, name)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("removing temp file %s: %w", path, err)
		}
	}

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
	logrus.Debug("flush is currently a no-op")
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
