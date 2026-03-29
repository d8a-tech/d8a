package spools

import (
	"encoding/binary"
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
type Spool interface {
	Append(key string, payload []byte) error
	Flush(fn func(key string, frames [][]byte) error) error
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

	return s.appendToFile(s.activePath(key), payload)
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
func (s *fileSpool) Flush(fn func(key string, frames [][]byte) error) error {
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
func (s *fileSpool) flushKey(key string, paths []string, fn func(string, [][]byte) error) error {
	for i, path := range paths {
		frames, readErr := readFrames(s.fs, path)
		if readErr != nil {
			return fmt.Errorf("reading inflight file %q: %w", path, readErr)
		}
		if len(frames) == 0 {
			s.removeInflight(path)
			continue
		}

		if fnErr := fn(key, frames); fnErr != nil {
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
