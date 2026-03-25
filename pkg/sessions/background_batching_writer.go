package sessions

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

// backgroundBatchingWriter implements SessionWriter with an injected SpoolWriter
// for the lvl1-to-lvl2 handoff and periodic lvl2 flushes to a child writer.
type backgroundBatchingWriter struct {
	childWriter     SessionWriter
	spoolWriter     SpoolWriter
	failureStrategy SpoolFailureStrategy

	// Configuration
	lvl2Dir                          string
	lvl2FlushInterval                time.Duration
	encoder                          encoding.EncoderFunc
	decoder                          encoding.DecoderFunc
	maxConsecutiveChildWriteFailures int

	// Actor pattern: single goroutine owns lvl2 state
	flushChan    chan struct{}
	stopChan     chan struct{}
	cleanupDone  chan struct{}
	actorStopped sync.WaitGroup

	ctx context.Context

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
}

// NewBackgroundBatchingWriter creates a writer that uses an injected SpoolWriter
// for the lvl1-to-lvl2 handoff and a SpoolFailureStrategy for exceeded failures.
// Returns the writer, a cleanup function (to be deferred), and an error.
func NewBackgroundBatchingWriter(
	ctx context.Context,
	childWriter SessionWriter,
	sw SpoolWriter,
	fs SpoolFailureStrategy,
	opts ...Option,
) (SessionWriter, func(), error) {
	if sw == nil {
		return nil, nil, fmt.Errorf("spool writer is required")
	}
	if fs == nil {
		return nil, nil, fmt.Errorf("spool failure strategy is required")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// Ensure lvl2 directory exists
	if err := os.MkdirAll(cfg.lvl2Dir, 0o750); err != nil {
		return nil, nil, fmt.Errorf("creating spool directory: %w", err)
	}

	w := &backgroundBatchingWriter{
		childWriter:                      childWriter,
		spoolWriter:                      sw,
		failureStrategy:                  fs,
		lvl2Dir:                          cfg.lvl2Dir,
		lvl2FlushInterval:                cfg.lvl2FlushInterval,
		encoder:                          cfg.encoder,
		decoder:                          cfg.decoder,
		maxConsecutiveChildWriteFailures: cfg.maxConsecutiveChildWriteFailures,
		flushChan:                        make(chan struct{}, 1),
		stopChan:                         make(chan struct{}),
		cleanupDone:                      make(chan struct{}),
		ctx:                              ctx,
	}

	w.actorStopped.Add(1)
	go w.actorLoop()

	cleanup := func() {
		w.closeOnce.Do(func() {
			w.mu.Lock()
			w.closed = true
			w.mu.Unlock()

			if err := w.spoolWriter.Close(); err != nil {
				logrus.Errorf("failed to close spool writer: %v", err)
			}

			close(w.stopChan)
			w.actorStopped.Wait()
			close(w.cleanupDone)
		})
	}

	return w, cleanup, nil
}

// Write implements SessionWriter by grouping sessions per property,
// encoding each group, and delegating to the injected SpoolWriter.
func (w *backgroundBatchingWriter) Write(sessions ...*schema.Session) error {
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return fmt.Errorf("writer is stopped")
	}
	w.mu.RUnlock()

	if len(sessions) == 0 {
		return nil
	}

	// Group sessions by property
	byProperty := make(map[string][]*schema.Session)
	for _, sess := range sessions {
		propID := sess.PropertyID
		if propID == "" {
			logrus.Warn("session has empty PropertyID, skipping")
			continue
		}
		byProperty[propID] = append(byProperty[propID], sess)
	}

	// Encode and delegate to spool writer per property
	for propID, propertySessions := range byProperty {
		var encodedBuf bytes.Buffer
		if _, err := w.encoder(&encodedBuf, propertySessions); err != nil {
			return fmt.Errorf("encoding sessions for property %q: %w", propID, err)
		}

		if err := w.spoolWriter.Write(propID, encodedBuf.Bytes()); err != nil {
			return fmt.Errorf("writing to spool for property %q: %w", propID, err)
		}
	}

	return nil
}

// actorLoop runs in a single goroutine and owns lvl2 state.
func (w *backgroundBatchingWriter) actorLoop() {
	defer w.actorStopped.Done()

	consecutiveFailuresBySpool := make(map[string]int)

	lvl2Ticker := time.NewTicker(w.lvl2FlushInterval)
	defer lvl2Ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			// Do NOT flush lvl2 to child on stop
			return

		case <-lvl2Ticker.C:
			w.flushLvl2ToChild(consecutiveFailuresBySpool)
		}
	}
}

// flushLvl2ToChild reads all spool files and flushes to child writer.
func (w *backgroundBatchingWriter) flushLvl2ToChild(consecutiveFailuresBySpool map[string]int) {
	entries, err := os.ReadDir(w.lvl2Dir)
	if err != nil {
		logrus.Errorf("failed to read spool directory: %v", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Only process .spool files
		if filepath.Ext(entry.Name()) != ".spool" {
			continue
		}

		spoolPath := filepath.Join(w.lvl2Dir, entry.Name())

		// Read and decode all records from this spool file
		allSessions, err := w.readSpoolFile(spoolPath)
		if err != nil {
			logrus.Errorf("failed to read spool file %q: %v", spoolPath, err)
			continue
		}

		if len(allSessions) == 0 {
			// Empty file, remove it
			if err := os.Remove(spoolPath); err != nil {
				logrus.Errorf("failed to remove empty spool file %q: %v", spoolPath, err)
			} else {
				delete(consecutiveFailuresBySpool, spoolPath)
			}
			continue
		}

		if err := w.childWriter.Write(allSessions...); err != nil {
			consecutiveFailuresBySpool[spoolPath]++
			failureCount := consecutiveFailuresBySpool[spoolPath]

			if failureCount >= w.maxConsecutiveChildWriteFailures {
				logrus.Errorf(
					"exceeded failure threshold for spool file %q after %d consecutive child writer failures (threshold: %d)",
					spoolPath,
					failureCount,
					w.maxConsecutiveChildWriteFailures,
				)
				if strategyErr := w.failureStrategy.OnExceededFailures(spoolPath); strategyErr != nil {
					logrus.Errorf("failure strategy error for spool file %q: %v", spoolPath, strategyErr)
					continue
				}
				delete(consecutiveFailuresBySpool, spoolPath)
				continue
			}

			logrus.Errorf("failed to write sessions from spool file %q to child writer: %v", spoolPath, err)
			continue
		}

		logrus.Debugf("wrote %d sessions from spool %q to warehouse", len(allSessions), spoolPath)

		// Success: remove spool file and reset failure count
		if err := os.Remove(spoolPath); err != nil {
			logrus.Errorf("failed to remove spool file %q after successful flush: %v", spoolPath, err)
		} else {
			delete(consecutiveFailuresBySpool, spoolPath)
		}
	}
}

// readSpoolFile reads all framed records from a spool file.
func (w *backgroundBatchingWriter) readSpoolFile(path string) ([]*schema.Session, error) {
	file, err := os.Open(filepath.Clean(path)) //nolint:gosec // path is constructed from property ID
	if err != nil {
		return nil, fmt.Errorf("opening spool file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			logrus.Errorf("failed to close spool file %q: %v", path, closeErr)
		}
	}()

	var allSessions []*schema.Session

	for {
		// Read header (4 bytes)
		header := make([]byte, 4)
		n, err := file.Read(header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading header: %w", err)
		}
		if n != 4 {
			logrus.Warnf("truncated header in spool file %q, stopping read", path)
			break
		}

		payloadLen := binary.LittleEndian.Uint32(header)

		// Read payload
		payload := make([]byte, payloadLen)
		n, err = file.Read(payload)
		if err == io.EOF {
			logrus.Warnf("truncated payload in spool file %q, stopping read", path)
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading payload: %w", err)
		}
		if n != int(payloadLen) {
			logrus.Warnf("truncated payload in spool file %q (expected %d, got %d), stopping read", path, payloadLen, n)
			break
		}

		// Decode payload
		var sessions []*schema.Session
		if err := w.decoder(bytes.NewReader(payload), &sessions); err != nil {
			return nil, fmt.Errorf("decoding payload: %w", err)
		}

		allSessions = append(allSessions, sessions...)
	}

	return allSessions, nil
}

// config holds configuration for the writer.
type config struct {
	lvl2Dir                          string
	lvl2FlushInterval                time.Duration
	encoder                          encoding.EncoderFunc
	decoder                          encoding.DecoderFunc
	maxConsecutiveChildWriteFailures int
}

func defaultConfig() *config {
	return &config{
		lvl2Dir:                          "/storage/writer",
		lvl2FlushInterval:                1 * time.Minute,
		encoder:                          encoding.GobEncoder,
		decoder:                          encoding.GobDecoder,
		maxConsecutiveChildWriteFailures: 20,
	}
}

// Option is a functional option for configuring the writer.
type Option func(*config)

// WithSpoolDir sets the directory for lvl2 spool files.
func WithSpoolDir(dir string) Option {
	return func(c *config) {
		c.lvl2Dir = dir
	}
}

// WithLvl2FlushInterval sets the flush interval for lvl2 to child writer.
func WithLvl2FlushInterval(d time.Duration) Option {
	return func(c *config) {
		c.lvl2FlushInterval = d
	}
}

// WithEncoderDecoder sets the encoder and decoder functions.
func WithEncoderDecoder(encoder encoding.EncoderFunc, decoder encoding.DecoderFunc) Option {
	return func(c *config) {
		c.encoder = encoder
		c.decoder = decoder
	}
}

// WithMaxConsecutiveChildWriteFailures sets the maximum number of consecutive
// child writer failures before the failure strategy is invoked.
func WithMaxConsecutiveChildWriteFailures(n int) Option {
	return func(c *config) {
		c.maxConsecutiveChildWriteFailures = n
	}
}
