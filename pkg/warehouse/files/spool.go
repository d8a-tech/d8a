package files

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/sirupsen/logrus"
)

// SpoolOption is a functional option for configuring SpoolDriver.
type SpoolOption func(*spoolDriver)

// withManualCycle disables automatic timer-based flush cycles (test-only).
func withManualCycle() SpoolOption {
	return func(sd *spoolDriver) {
		sd.sealCheckInterval = 0
	}
}

// WithMaxSegmentSize sets the maximum segment size in bytes before sealing.
func WithMaxSegmentSize(n int64) SpoolOption {
	return func(sd *spoolDriver) {
		sd.maxSegmentSize = n
	}
}

// WithMaxSegmentAge sets the maximum segment age before sealing.
func WithMaxSegmentAge(d time.Duration) SpoolOption {
	return func(sd *spoolDriver) {
		sd.maxSegmentAge = d
	}
}

// WithSealCheckInterval sets how often to evaluate sealing triggers.
func WithSealCheckInterval(d time.Duration) SpoolOption {
	return func(sd *spoolDriver) {
		sd.sealCheckInterval = d
	}
}

// ticker abstracts time.Ticker to allow deterministic testing.
type ticker interface {
	C() <-chan time.Time
	Stop()
}

// realTicker wraps *time.Ticker to implement ticker.
type realTicker struct {
	t *time.Ticker
}

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

// spoolDriver is a warehouse.Driver that writes analytics data directly to disk
// files and periodically uploads them to object storage.
//
// Disk is the source of truth for persisted data. An in-memory streams map
// maintains per-stream state (createdAt, activeSizeBytes) used to evaluate
// sealing triggers.
type spoolDriver struct {
	ctx               context.Context
	uploader          Uploader
	format            Format
	ext               string // file extension without leading dot, derived from format
	spoolDir          string
	maxSegmentSize    int64
	maxSegmentAge     time.Duration
	sealCheckInterval time.Duration
	stopCh            chan struct{}  // signal to stop timer
	stopOnce          sync.Once      // ensures stopCh is closed only once
	wg                sync.WaitGroup // wait for timer goroutine
	mu                sync.Mutex     // protects concurrent file operations
	streams           map[string]*streamState
	newTicker         func(time.Duration) ticker
}

var _ warehouse.Driver = (*spoolDriver)(nil)

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
	opts ...SpoolOption,
) *spoolDriver {
	sd := &spoolDriver{
		ctx:               ctx,
		uploader:          uploader,
		format:            format,
		ext:               format.Extension(),
		spoolDir:          spoolDir,
		maxSegmentSize:    1 << 30,   // 1 GiB
		maxSegmentAge:     time.Hour, // 1 hour
		sealCheckInterval: 15 * time.Second,
		stopCh:            make(chan struct{}),
		streams:           make(map[string]*streamState),
	}

	sd.newTicker = func(d time.Duration) ticker {
		return &realTicker{time.NewTicker(d)}
	}

	for _, opt := range opts {
		opt(sd)
	}

	// Unrecoverable initialization failure - if we can't recover streams,
	// the driver cannot safely operate or ensure data consistency.
	if err := sd.recoverStreams(); err != nil {
		logrus.WithError(err).Panic("failed to recover streams on startup")
	}

	sd.startTimer()

	return sd
}

// startTimer starts the periodic flush cycle timer (if interval > 0).
func (sd *spoolDriver) startTimer() {
	if sd.sealCheckInterval == 0 {
		return
	}

	sd.wg.Add(1)
	go func() {
		defer sd.wg.Done()
		logrus.WithField("interval", sd.sealCheckInterval).Info("started flush cycle timer")

		tk := sd.newTicker(sd.sealCheckInterval)
		defer tk.Stop()

		for {
			select {
			case <-tk.C():
				if err := sd.runFlushCycle(sd.ctx, false); err != nil {
					logrus.WithError(err).Error("automatic flush cycle failed")
				}
			case <-sd.ctx.Done():
				return
			case <-sd.stopCh:
				return
			}
		}
	}()
}

// Write appends rows to disk file immediately.
// All file operations are mutex-protected to prevent concurrent writes.
func (sd *spoolDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName(table)
	csvPath := activePath(sd.spoolDir, tableEsc, fingerprint)
	key := streamKey(tableEsc, fingerprint)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	if err := ensureStreamDirs(sd.spoolDir, tableEsc, fingerprint); err != nil {
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

	return nil
}

func streamKey(tableEsc, fingerprint string) string {
	return fmt.Sprintf("%s/%s", tableEsc, fingerprint)
}

func (sd *spoolDriver) recoverUploading(tableEsc, fingerprint string) error {
	uploadingDir := uploadingDir(sd.spoolDir, tableEsc, fingerprint)
	entries, err := os.ReadDir(uploadingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading uploading dir %s: %w", uploadingDir, err)
	}

	sealedDir := sealedDir(sd.spoolDir, tableEsc, fingerprint)
	dotExt := "." + sd.ext
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) != dotExt {
			continue
		}

		segmentID := name[:len(name)-len(dotExt)]
		uploadingPath := filepath.Join(uploadingDir, name)
		sealedPath := filepath.Join(sealedDir, name)
		if err := os.Rename(uploadingPath, sealedPath); err != nil {
			return fmt.Errorf("moving uploading segment %s: %w", uploadingPath, err)
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

type sealedSegment struct {
	tableEsc  string
	fp        string
	segmentID string
	sealTime  time.Time
}

// runFlushCycle evaluates triggers, seals segments, and uploads sealed segments.
func (sd *spoolDriver) runFlushCycle(ctx context.Context, forceAll bool) error {
	justSealed, err := sd.evaluateAndSeal(forceAll)
	if err != nil {
		return err
	}

	toUpload, err := sd.discoverAllSealed(justSealed)
	if err != nil {
		return err
	}

	var errs []error
	for _, seg := range toUpload {
		if err := sd.uploadSegment(ctx, seg); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// evaluateAndSeal checks all active streams for seal triggers and seals those that match.
func (sd *spoolDriver) evaluateAndSeal(forceAll bool) ([]sealedSegment, error) {
	var toSeal []sealedSegment

	sd.mu.Lock()
	defer sd.mu.Unlock()

	for key, state := range sd.streams {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}
		tableEsc, fp := parts[0], parts[1]

		shouldSeal := forceAll ||
			(state.activeSizeBytes > 0 &&
				(state.activeSizeBytes >= sd.maxSegmentSize ||
					time.Since(state.createdAt) >= sd.maxSegmentAge))

		if shouldSeal {
			segmentID, sealTime, err := sd.sealStream(tableEsc, fp)
			if err != nil {
				return nil, fmt.Errorf("sealing stream %s: %w", key, err)
			}
			toSeal = append(toSeal, sealedSegment{
				tableEsc:  tableEsc,
				fp:        fp,
				segmentID: segmentID,
				sealTime:  sealTime,
			})
		}
	}
	return toSeal, nil
}

// discoverAllSealed merges justSealed with any crash-recovered segments on disk.
func (sd *spoolDriver) discoverAllSealed(justSealed []sealedSegment) ([]sealedSegment, error) {
	toUpload := make([]sealedSegment, 0, len(justSealed))
	sealedSet := make(map[string]struct{})
	for _, s := range justSealed {
		sealedSet[s.tableEsc+"/"+s.fp+"/"+s.segmentID] = struct{}{}
		toUpload = append(toUpload, s)
	}

	streamsRoot := filepath.Join(sd.spoolDir, "streams")
	tableEntries, err := os.ReadDir(streamsRoot)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading streams root: %w", err)
	}

	for _, tableEntry := range tableEntries {
		if !tableEntry.IsDir() {
			continue
		}
		tableEsc := tableEntry.Name()
		tableDir := filepath.Join(streamsRoot, tableEsc)
		fpEntries, err := os.ReadDir(tableDir)
		if err != nil {
			return nil, fmt.Errorf("reading table dir %s: %w", tableDir, err)
		}

		for _, fpEntry := range fpEntries {
			if !fpEntry.IsDir() {
				continue
			}
			fp := fpEntry.Name()
			sealedDir := sealedDir(sd.spoolDir, tableEsc, fp)
			segments, err := findSealedSegments(sealedDir, sd.ext)
			if err != nil {
				return nil, fmt.Errorf("finding sealed segments in %s: %w", sealedDir, err)
			}

			for _, segmentID := range segments {
				key := tableEsc + "/" + fp + "/" + segmentID
				if _, ok := sealedSet[key]; ok {
					continue // already in toUpload from justSealed
				}
				sealTime, ok := parseSealTimeFromSegmentID(segmentID)
				if !ok {
					// Legacy bare-UUID segment: use file modtime for stable dt= partitioning.
					segPath := segmentPath(sealedDir, segmentID, sd.ext)
					info, statErr := os.Stat(segPath)
					if statErr == nil {
						sealTime = info.ModTime().UTC()
					} else {
						sealTime = time.Now().UTC()
					}
				}
				toUpload = append(toUpload, sealedSegment{
					tableEsc:  tableEsc,
					fp:        fp,
					segmentID: segmentID,
					sealTime:  sealTime,
				})
			}
		}
	}
	return toUpload, nil
}

func (sd *spoolDriver) uploadSegment(ctx context.Context, seg sealedSegment) error {
	streamDir := streamDir(sd.spoolDir, seg.tableEsc, seg.fp)
	sealedDir := sealedDir(sd.spoolDir, seg.tableEsc, seg.fp)
	uploadingDir := uploadingDir(sd.spoolDir, seg.tableEsc, seg.fp)
	sealedPath := segmentPath(sealedDir, seg.segmentID, sd.ext)
	uploadingPath := segmentPath(uploadingDir, seg.segmentID, sd.ext)

	// Move sealed -> uploading under lock
	sd.mu.Lock()
	if err := os.Rename(sealedPath, uploadingPath); err != nil {
		sd.mu.Unlock()
		if os.IsNotExist(err) {
			// Segment was already uploaded or moved by another process
			return nil
		}
		return fmt.Errorf("moving segment to uploading: %w", err)
	}
	sd.mu.Unlock()

	// Upload outside lock
	remoteKey := segmentRemoteKey(seg.tableEsc, seg.fp, seg.segmentID, sd.ext, seg.sealTime)
	uploadErr := sd.uploader.Upload(ctx, uploadingPath, remoteKey)

	if uploadErr == nil {
		// Success: remove the uploaded file
		if err := os.Remove(uploadingPath); err != nil && !os.IsNotExist(err) {
			logrus.WithError(err).WithField("path", uploadingPath).Warn("failed to remove uploaded file")
		}
		logrus.WithFields(logrus.Fields{
			"table":      seg.tableEsc,
			"fp":         seg.fp,
			"segment_id": seg.segmentID,
			"remote_key": remoteKey,
		}).Info("uploaded segment")
		return nil
	}

	// Failure: move back to sealed, increment fail count
	sd.mu.Lock()
	if err := os.Rename(uploadingPath, sealedPath); err != nil {
		sd.mu.Unlock()
		return fmt.Errorf("moving segment back to sealed after upload failure: %w", err)
	}
	sd.mu.Unlock()

	failCount := readFailCount(streamDir, seg.segmentID) + 1
	if writeErr := writeFailCount(streamDir, seg.segmentID, failCount); writeErr != nil {
		logrus.WithError(writeErr).WithField("segment_id", seg.segmentID).Warn("failed to write fail count")
	}

	if failCount >= 3 {
		if err := sd.quarantine(seg.tableEsc, seg.fp, seg.segmentID); err != nil {
			logrus.WithError(err).WithField("segment_id", seg.segmentID).Error("failed to quarantine segment")
		} else {
			logrus.WithFields(logrus.Fields{
				"table":      seg.tableEsc,
				"fp":         seg.fp,
				"segment_id": seg.segmentID,
			}).Warn("quarantined segment after 3 failures")
		}
	} else {
		logrus.WithFields(logrus.Fields{
			"table":      seg.tableEsc,
			"fp":         seg.fp,
			"segment_id": seg.segmentID,
			"fail_count": failCount,
			"error":      uploadErr,
		}).Warn("upload failed, will retry")
	}

	return uploadErr
}

func readFailCount(streamDir, segmentID string) int {
	path := failCountPath(streamDir, segmentID)
	data, err := os.ReadFile(path) //nolint:gosec // path is controlled
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return n
}

func writeFailCount(streamDir, segmentID string, n int) error {
	path := failCountPath(streamDir, segmentID)
	tmpPath := path + ".tmp"
	content := []byte(strconv.Itoa(n))
	if err := os.WriteFile(tmpPath, content, 0o600); err != nil {
		return fmt.Errorf("writing fail count temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("renaming fail count file: %w", err)
	}
	return nil
}

func (sd *spoolDriver) quarantine(tableEsc, fp, segmentID string) error {
	sealedDir := sealedDir(sd.spoolDir, tableEsc, fp)
	failedDir := failedDir(sd.spoolDir, tableEsc, fp)
	streamDir := streamDir(sd.spoolDir, tableEsc, fp)

	sealedPath := segmentPath(sealedDir, segmentID, sd.ext)
	failedPath := segmentPath(failedDir, segmentID, sd.ext)

	if err := os.Rename(sealedPath, failedPath); err != nil {
		return fmt.Errorf("moving segment to failed: %w", err)
	}

	// Remove failcount file
	failCountPath := failCountPath(streamDir, segmentID)
	if err := os.Remove(failCountPath); err != nil && !os.IsNotExist(err) {
		logrus.WithError(err).WithField("segment_id", segmentID).Warn("failed to remove failcount file")
	}

	return nil
}

// sealStream must be called with sd.mu held.
func (sd *spoolDriver) sealStream(tableEsc, fingerprint string) (segmentID string, sealTime time.Time, err error) {
	sealTime = time.Now().UTC()
	segmentID = segmentIDFromSealTime(sealTime)

	activePath := activePath(sd.spoolDir, tableEsc, fingerprint)
	sealedDir := sealedDir(sd.spoolDir, tableEsc, fingerprint)
	sealedPath := segmentPath(sealedDir, segmentID, sd.ext)
	if err := os.Rename(activePath, sealedPath); err != nil {
		return "", time.Time{}, fmt.Errorf("sealing active segment: %w", err)
	}

	delete(sd.streams, streamKey(tableEsc, fingerprint))

	return segmentID, sealTime, nil
}

// Close gracefully shuts down the driver.
func (sd *spoolDriver) Close() error {
	sd.stopOnce.Do(func() { close(sd.stopCh) })
	sd.wg.Wait()
	err := sd.runFlushCycle(context.Background(), true)
	logrus.Info("driver closed")
	return err
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

			activePath := activePath(sd.spoolDir, tableEsc, fingerprint)
			activeInfo, err := os.Stat(activePath)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return fmt.Errorf("statting active CSV %s: %w", activePath, err)
			}

			key := streamKey(tableEsc, fingerprint)
			sd.streams[key] = &streamState{
				createdAt:       activeInfo.ModTime().UTC(),
				activeSizeBytes: activeInfo.Size(),
			}
		}
	}

	return nil
}
