package spools

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const (
	headerSize        = 4
	spoolExt          = ".spool"
	inflightMarker    = ".spool.inflight."
	filePerms         = 0o644
	maxPayloadSz      = 0xFFFFFFFF
	defaultMaxFailure = 20
)

// Spool is a crash-safe keyed append-only framed-file store.
// Append and Flush are safe to call concurrently with each other.
//
// The Flush callback receives a next function that yields successive [][]byte
// batches from the current inflight file. next returns io.EOF when the file
// is exhausted. If the callback returns an error, the entire inflight file is
// retained for retry on the next Flush cycle.
type Spool interface {
	Append(key string, payload []byte) error
	Flush(fn func(key string, next func() ([][]byte, error)) error) error
	Recover() error
	Close() error
}

// FailureStrategy defines the action to take when an inflight spool file
// exceeds the maximum number of consecutive flush failures.
type FailureStrategy interface {
	OnExceededFailures(fs afero.Fs, inflightPath string) error
}

// deleteStrategy removes the inflight file on exceeded failures (best-effort delivery).
type deleteStrategy struct{}

// NewDeleteStrategy creates a FailureStrategy that deletes inflight files
// when consecutive flush failures exceed the threshold.
func NewDeleteStrategy() FailureStrategy {
	return &deleteStrategy{}
}

// OnExceededFailures implements FailureStrategy.
func (s *deleteStrategy) OnExceededFailures(fs afero.Fs, inflightPath string) error {
	if err := fs.Remove(inflightPath); err != nil {
		return fmt.Errorf("removing discarded spool file %q: %w", inflightPath, err)
	}
	return nil
}

// quarantineStrategy renames the inflight file to .quarantine on exceeded failures (at-least-once delivery).
type quarantineStrategy struct{}

// NewQuarantineStrategy creates a FailureStrategy that quarantines inflight files
// by renaming them with a .quarantine suffix when consecutive flush failures
// exceed the threshold.
func NewQuarantineStrategy() FailureStrategy {
	return &quarantineStrategy{}
}

// OnExceededFailures implements FailureStrategy.
func (s *quarantineStrategy) OnExceededFailures(fs afero.Fs, inflightPath string) error {
	quarantinePath := inflightPath + ".quarantine"
	if err := fs.Rename(inflightPath, quarantinePath); err != nil {
		return fmt.Errorf("quarantining spool file %q: %w", inflightPath, err)
	}
	logrus.Warnf("quarantined spool file %q to %q after exceeding failure threshold", inflightPath, quarantinePath)
	return nil
}

// Option configures a fileSpool.
type Option func(*fileSpool)

// WithFailureStrategy sets the strategy invoked when an inflight file
// exceeds the maximum number of consecutive flush failures.
// When nil (the default), inflight files are deleted on threshold breach.
func WithFailureStrategy(s FailureStrategy) Option {
	return func(f *fileSpool) {
		f.failureStrategy = s
	}
}

// WithMaxFailures sets the per-key consecutive failure threshold
// before the failure strategy is invoked. Default is 20.
func WithMaxFailures(n int) Option {
	return func(f *fileSpool) {
		f.maxFailures = n
	}
}

// WithNowFunc overrides the clock used to generate inflight timestamps.
// Intended for testing.
func WithNowFunc(fn func() time.Time) Option {
	return func(f *fileSpool) {
		f.nowFunc = fn
	}
}

// WithMaxActiveSize sets the maximum size (in bytes) for an active spool
// file. When an Append would cause the active file to exceed this limit,
// the current active file is rotated to a sealed inflight file and a fresh
// active file is started. Zero (default) means no size limit.
func WithMaxActiveSize(bytes int64) Option {
	return func(f *fileSpool) {
		f.maxActiveSize = bytes
	}
}

// WithFlushBatchSize sets the maximum number of frames returned per call
// to the next function passed to the Flush callback. Zero (default) means
// all frames in the inflight file are returned in a single batch.
func WithFlushBatchSize(n int) Option {
	return func(f *fileSpool) {
		f.flushBatchSize = n
	}
}

// New creates a Spool backed by fs in the given directory.
// It calls Recover before returning to re-ingest any crash remnants.
func New(fs afero.Fs, dir string, opts ...Option) (Spool, error) {
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating spool directory: %w", err)
	}

	s := &fileSpool{
		fs:              fs,
		dir:             dir,
		failureStrategy: &deleteStrategy{},
		maxFailures:     defaultMaxFailure,
		failuresByKey:   make(map[string]int),
		nowFunc:         time.Now,
	}
	for _, o := range opts {
		o(s)
	}

	if err := s.Recover(); err != nil {
		return nil, fmt.Errorf("recovering spool: %w", err)
	}
	return s, nil
}

type fileSpool struct {
	fs              afero.Fs
	dir             string
	failureStrategy FailureStrategy
	maxFailures     int
	maxActiveSize   int64
	flushBatchSize  int
	nowFunc         func() time.Time
	mu              sync.Mutex
	closed          bool
	failMu          sync.Mutex // guards failuresByKey independently of mu
	failuresByKey   map[string]int
}

func sanitizeKey(key string) string {
	return strings.NewReplacer("/", "_", "\\", "_").Replace(key)
}

func (s *fileSpool) activePath(key string) string {
	return s.dir + "/" + sanitizeKey(key) + spoolExt
}

// isInflightFile reports whether name is an inflight spool file.
func isInflightFile(name string) bool {
	return strings.Contains(name, inflightMarker)
}

// isActiveFile reports whether name is an active spool file (not inflight).
func isActiveFile(name string) bool {
	return strings.HasSuffix(name, spoolExt) && !isInflightFile(name)
}

// keyFromInflight extracts the key from an inflight filename
// (everything before the inflightMarker).
func keyFromInflight(name string) string {
	idx := strings.Index(name, inflightMarker)
	if idx < 0 {
		return ""
	}
	return name[:idx]
}

// Append implements Spool.
func (s *fileSpool) Append(key string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("spool is closed")
	}

	if len(payload) > maxPayloadSz {
		return fmt.Errorf("payload too large for key %q: %d bytes", key, len(payload))
	}

	if err := s.appendToFile(s.activePath(key), payload); err != nil {
		return err
	}

	if s.maxActiveSize > 0 {
		if err := s.maybeRotateActive(key); err != nil {
			return err
		}
	}

	return nil
}

// maybeRotateActive checks if the active file for key exceeds maxActiveSize
// and renames it to an inflight file if so. Must be called with s.mu held.
func (s *fileSpool) maybeRotateActive(key string) error {
	path := s.activePath(key)
	info, err := s.fs.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat active file %q: %w", path, err)
	}
	if info.Size() < s.maxActiveSize {
		return nil
	}
	inflight := fmt.Sprintf("%s/%s%s%d", s.dir, sanitizeKey(key), inflightMarker, s.nowFunc().UnixNano())
	if err := s.fs.Rename(path, inflight); err != nil {
		return fmt.Errorf("rotating active file %q: %w", path, err)
	}
	return nil
}

func (s *fileSpool) appendToFile(path string, payload []byte) error {
	f, err := s.fs.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerms)
	if err != nil {
		return fmt.Errorf("opening spool file %q: %w", path, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			logrus.Errorf("closing spool file %q: %v", path, closeErr)
		}
	}()

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header, uint32(len(payload))) //nolint:gosec // checked above
	if _, err := f.Write(header); err != nil {
		return fmt.Errorf("writing header to %q: %w", path, err)
	}
	if _, err := f.Write(payload); err != nil {
		return fmt.Errorf("writing payload to %q: %w", path, err)
	}

	if syncer, ok := f.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return fmt.Errorf("syncing spool file %q: %w", path, err)
		}
	}

	return nil
}

// collectInflight renames active spool files to inflight and collects all
// inflight files grouped by key. Must be called with s.mu held; the caller
// must release it after this returns.
// Returns a map of key -> sorted inflight paths (oldest first).
func (s *fileSpool) collectInflight() (map[string][]string, error) {
	entries, err := afero.ReadDir(s.fs, s.dir)
	if err != nil {
		return nil, fmt.Errorf("reading spool directory: %w", err)
	}

	// Rename every active file to a timestamped inflight — no deferral.
	for _, entry := range entries {
		name := entry.Name()
		if !isActiveFile(name) {
			continue
		}
		key := strings.TrimSuffix(name, spoolExt)
		active := s.dir + "/" + name
		inflight := fmt.Sprintf("%s/%s%s%d", s.dir, key, inflightMarker, s.nowFunc().UnixNano())

		if err := s.fs.Rename(active, inflight); err != nil {
			return nil, fmt.Errorf("renaming %q to inflight: %w", active, err)
		}
	}

	// Re-read to pick up all inflight files (old + just-renamed).
	entries, err = afero.ReadDir(s.fs, s.dir)
	if err != nil {
		return nil, fmt.Errorf("reading spool directory after rename: %w", err)
	}

	byKey := make(map[string][]string)
	for _, entry := range entries {
		name := entry.Name()
		if !isInflightFile(name) {
			continue
		}
		key := keyFromInflight(name)
		byKey[key] = append(byKey[key], s.dir+"/"+name)
	}

	// Sort each key's inflight paths lexicographically (oldest tsnano first).
	for _, paths := range byKey {
		sort.Strings(paths)
	}

	return byKey, nil
}

// Flush implements Spool.
func (s *fileSpool) Flush(fn func(key string, next func() ([][]byte, error)) error) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("spool is closed")
	}

	byKey, err := s.collectInflight()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	// Process each key's inflight files in order (oldest first).
	var flushErr error
	for key, paths := range byKey {
		if err := s.flushKey(key, paths, fn); err != nil {
			flushErr = err
		}
	}

	return flushErr
}

// flushKey processes inflight files for a single key in order.
// Stops on the first failure (preserves per-key ordering).
func (s *fileSpool) flushKey(key string, paths []string, fn func(string, func() ([][]byte, error)) error) error {
	for i, path := range paths {
		if err := s.flushOneInflight(key, path); err != nil {
			if errors.Is(err, errEmptyInflight) {
				continue
			}
			return fmt.Errorf("reading inflight file %q: %w", path, err)
		}

		next, cleanup := s.makeNextFunc(path)
		fnErr := fn(key, next)
		cleanup()

		if fnErr != nil {
			// Pass only the remaining (unprocessed) paths to the failure handler.
			return s.handleFlushFailure(key, paths[i:], fnErr)
		}

		// Success — reset failure counter and remove the inflight file.
		s.failMu.Lock()
		delete(s.failuresByKey, key)
		s.failMu.Unlock()
		s.removeInflight(path)
	}
	return nil
}

// sentinel for empty inflight files that should be skipped and removed.
var errEmptyInflight = fmt.Errorf("empty inflight file")

// flushOneInflight checks whether an inflight file has frames. If empty,
// it removes the file and returns errEmptyInflight.
func (s *fileSpool) flushOneInflight(key, path string) error {
	_ = key
	info, err := s.fs.Stat(path)
	if err != nil {
		return fmt.Errorf("stat inflight %q: %w", path, err)
	}
	if info.Size() == 0 {
		s.removeInflight(path)
		return errEmptyInflight
	}
	return nil
}

// frameReader reads framed records incrementally from an open file.
type frameReader struct {
	file   afero.File
	path   string
	header []byte
	done   bool
}

// newFrameReader opens the file at path for incremental frame reading.
func newFrameReader(fs afero.Fs, path string) (*frameReader, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %q: %w", path, err)
	}
	return &frameReader{
		file:   f,
		path:   path,
		header: make([]byte, headerSize),
	}, nil
}

// readFrame reads a single frame from the file. Returns io.EOF when there are
// no more frames. Truncated trailing frames are tolerated: a warning is logged
// and io.EOF is returned.
func (r *frameReader) readFrame() ([]byte, error) {
	if r.done {
		return nil, io.EOF
	}

	_, err := io.ReadFull(r.file, r.header)
	if err != nil {
		if err == io.EOF {
			r.done = true
			return nil, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			logrus.Warnf("truncated frame header in %q; stopping incremental read", r.path)
			r.done = true
			return nil, io.EOF
		}
		return nil, fmt.Errorf("reading frame header from %q: %w", r.path, err)
	}

	size := binary.LittleEndian.Uint32(r.header)
	payload := make([]byte, size)
	_, err = io.ReadFull(r.file, payload)
	if err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			logrus.Warnf("truncated frame payload in %q (expected %d bytes); stopping incremental read",
				r.path, size)
			r.done = true
			return nil, io.EOF
		}
		return nil, fmt.Errorf("reading frame payload from %q: %w", r.path, err)
	}

	return payload, nil
}

// close closes the underlying file.
func (r *frameReader) close() {
	if err := r.file.Close(); err != nil {
		logrus.Errorf("closing frame reader for %q: %v", r.path, err)
	}
}

// makeNextFunc returns a next closure that reads frames from the inflight file
// incrementally, plus a cleanup function that must be called when the caller is
// done with the iterator (regardless of whether it was fully drained).
// The file is opened lazily on the first next() call.
// When all frames are consumed next returns io.EOF.
// If flushBatchSize is 0 (default), all remaining frames are returned in a single batch.
func (s *fileSpool) makeNextFunc(path string) (next func() ([][]byte, error), cleanup func()) {
	var (
		reader    *frameReader
		exhausted bool
	)

	closeReader := func() {
		if reader != nil {
			reader.close()
			reader = nil
		}
	}

	next = func() ([][]byte, error) {
		if exhausted {
			return nil, io.EOF
		}

		if reader == nil {
			var err error
			reader, err = newFrameReader(s.fs, path)
			if err != nil {
				return nil, fmt.Errorf("reading frames from %q: %w", path, err)
			}
		}

		batch, err := s.readBatch(reader)
		if err != nil {
			closeReader()
			return nil, err
		}

		if len(batch) == 0 {
			closeReader()
			exhausted = true
			return nil, io.EOF
		}

		return batch, nil
	}

	return next, closeReader
}

// readBatch reads up to s.flushBatchSize frames from the reader.
// Returns an empty slice when no more frames are available.
func (s *fileSpool) readBatch(r *frameReader) ([][]byte, error) {
	var batch [][]byte

	for {
		frame, err := r.readFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return batch, nil
			}
			return nil, err
		}

		batch = append(batch, frame)

		if s.flushBatchSize > 0 && len(batch) >= s.flushBatchSize {
			return batch, nil
		}
	}
}

func (s *fileSpool) removeInflight(path string) {
	if err := s.fs.Remove(path); err != nil && !os.IsNotExist(err) {
		logrus.Errorf("removing inflight file %q: %v", path, err)
	}
}

func (s *fileSpool) handleFlushFailure(key string, allPaths []string, fnErr error) error {
	s.failMu.Lock()
	s.failuresByKey[key]++
	count := s.failuresByKey[key]
	s.failMu.Unlock()

	if count >= s.maxFailures {
		logrus.Errorf(
			"exceeded failure threshold for key %q after %d consecutive failures (threshold: %d)",
			key, count, s.maxFailures,
		)
		// Invoke strategy on ALL inflight files for this key.
		for _, p := range allPaths {
			if stratErr := s.failureStrategy.OnExceededFailures(s.fs, p); stratErr != nil {
				logrus.Errorf("failure strategy error for key %q, file %q: %v", key, p, stratErr)
				return fmt.Errorf("failure strategy for key %q: %w", key, stratErr)
			}
		}
		s.failMu.Lock()
		delete(s.failuresByKey, key)
		s.failMu.Unlock()
		return nil
	}

	// Leave inflight files in place for retry on next flush cycle.
	logrus.Warnf("flush callback failed for key %q (%d/%d): %v", key, count, s.maxFailures, fnErr)
	return fnErr
}

// Recover implements Spool.
func (s *fileSpool) Recover() error {
	entries, err := afero.ReadDir(s.fs, s.dir)
	if err != nil {
		return fmt.Errorf("reading spool directory for recovery: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if !isInflightFile(name) {
			continue
		}

		key := keyFromInflight(name)
		inflightPath := s.dir + "/" + name

		frames, readErr := readFrames(s.fs, inflightPath)
		if readErr != nil {
			return fmt.Errorf("reading inflight file %q during recovery: %w", inflightPath, readErr)
		}

		activePath := s.activePath(key)
		for _, frame := range frames {
			if appendErr := s.appendToFile(activePath, frame); appendErr != nil {
				return fmt.Errorf("re-appending frame to %q during recovery: %w", activePath, appendErr)
			}
		}

		if removeErr := s.fs.Remove(inflightPath); removeErr != nil {
			return fmt.Errorf("removing recovered inflight file %q: %w", inflightPath, removeErr)
		}
	}

	return nil
}

// Close implements Spool.
func (s *fileSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// readFrames reads all framed records from the file at path.
// On a truncated trailing frame it logs a warning and returns the frames read so far.
func readFrames(fs afero.Fs, path string) ([][]byte, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %q: %w", path, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			logrus.Errorf("closing %q: %v", path, closeErr)
		}
	}()

	var frames [][]byte
	header := make([]byte, headerSize)

	for {
		_, err := io.ReadFull(f, header)
		if err != nil {
			if err == io.EOF {
				break
			}
			// Short header read — truncated trailing frame.
			if err == io.ErrUnexpectedEOF {
				logrus.Warnf("truncated frame header in %q; stopping read with %d frames recovered", path, len(frames))
				break
			}
			return nil, fmt.Errorf("reading frame header from %q: %w", path, err)
		}

		size := binary.LittleEndian.Uint32(header)
		payload := make([]byte, size)
		_, err = io.ReadFull(f, payload)
		if err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				logrus.Warnf("truncated frame payload in %q (expected %d bytes); stopping read with %d frames recovered",
					path, size, len(frames))
				break
			}
			return nil, fmt.Errorf("reading frame payload from %q: %w", path, err)
		}

		frames = append(frames, payload)
	}

	return frames, nil
}
