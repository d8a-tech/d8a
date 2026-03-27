package spools

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const (
	headerSize   = 4
	spoolExt     = ".spool"
	inflightExt  = ".spool.inflight"
	filePerms    = 0o644
	maxPayloadSz = 0xFFFFFFFF
)

// Spool is a crash-safe keyed append-only framed-file store.
// Append and Flush are safe to call concurrently with each other.
type Spool interface {
	Append(key string, payload []byte) error
	Flush(fn func(key string, inflightPath string, frames [][]byte) error) error
	Recover() error
	Close() error
}

// Option configures a fileSpool.
type Option func(*fileSpool)

// New creates a Spool backed by fs in the given directory.
// It calls Recover before returning to re-ingest any crash remnants.
func New(fs afero.Fs, dir string, opts ...Option) (Spool, error) {
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating spool directory: %w", err)
	}

	s := &fileSpool{
		fs:  fs,
		dir: dir,
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
	fs     afero.Fs
	dir    string
	mu     sync.Mutex
	closed bool
}

func sanitizeKey(key string) string {
	return strings.NewReplacer("/", "_", "\\", "_").Replace(key)
}

func (s *fileSpool) activePath(key string) string {
	return s.dir + "/" + sanitizeKey(key) + spoolExt
}

// keyFromFilename extracts the original sanitized key from a spool filename.
func keyFromFilename(name, ext string) string {
	return strings.TrimSuffix(name, ext)
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

// Flush implements Spool.
func (s *fileSpool) Flush(fn func(key string, inflightPath string, frames [][]byte) error) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("spool is closed")
	}

	entries, err := afero.ReadDir(s.fs, s.dir)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("reading spool directory: %w", err)
	}

	// Collect active files and rename them to inflight while holding the lock.
	// Also pick up pre-existing inflight files left by a prior failed flush.
	type pending struct {
		key          string
		inflightPath string
	}
	var toFlush []pending
	seen := make(map[string]bool)

	// First pass: rename active files to inflight.
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, spoolExt) || strings.HasSuffix(name, inflightExt) {
			continue
		}
		key := keyFromFilename(name, spoolExt)
		active := s.dir + "/" + name
		inflight := active + ".inflight"

		if err := s.fs.Rename(active, inflight); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("renaming %q to inflight: %w", active, err)
		}
		toFlush = append(toFlush, pending{key: key, inflightPath: inflight})
		seen[inflight] = true
	}

	// Second pass: collect pre-existing inflight files from prior failed flushes.
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, inflightExt) {
			continue
		}
		inflightPath := s.dir + "/" + name
		if seen[inflightPath] {
			continue
		}
		key := keyFromFilename(name, inflightExt)
		toFlush = append(toFlush, pending{key: key, inflightPath: inflightPath})
	}
	s.mu.Unlock()

	// Process inflight files without holding the lock.
	var flushErr error
	for _, p := range toFlush {
		frames, readErr := readFrames(s.fs, p.inflightPath)
		if readErr != nil {
			return fmt.Errorf("reading inflight file %q: %w", p.inflightPath, readErr)
		}
		if len(frames) == 0 {
			// Empty file — just remove it.
			if removeErr := s.fs.Remove(p.inflightPath); removeErr != nil {
				logrus.Errorf("removing empty inflight file %q: %v", p.inflightPath, removeErr)
			}
			continue
		}

		if fnErr := fn(p.key, p.inflightPath, frames); fnErr != nil {
			// Leave inflight file in place for retry on next flush cycle.
			logrus.Warnf("flush callback failed for key %q: %v", p.key, fnErr)
			flushErr = fnErr
			continue
		}

		if removeErr := s.fs.Remove(p.inflightPath); removeErr != nil {
			logrus.Errorf("removing inflight file %q after successful flush: %v", p.inflightPath, removeErr)
		}
	}

	return flushErr
}

// Recover implements Spool.
func (s *fileSpool) Recover() error {
	entries, err := afero.ReadDir(s.fs, s.dir)
	if err != nil {
		return fmt.Errorf("reading spool directory for recovery: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, inflightExt) {
			continue
		}

		key := keyFromFilename(name, inflightExt)
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
