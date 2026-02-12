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

// backgroundBatchingWriter implements SessionWriter with two-level batching:
// - Lvl1: per-property in-memory buffer (flushed on age/count)
// - Lvl2: per-property on-disk spool files (flushed periodically to child writer)
type backgroundBatchingWriter struct {
	childWriter SessionWriter

	// Configuration
	lvl1MaxAge                       time.Duration
	lvl1MaxSessions                  int
	lvl1SweepInterval                time.Duration
	lvl2Dir                          string
	lvl2FlushInterval                time.Duration
	encoder                          encoding.EncoderFunc
	decoder                          encoding.DecoderFunc
	maxConsecutiveChildWriteFailures int

	// Actor pattern: single goroutine owns all state
	writeChan    chan writeRequest
	flushChan    chan struct{}
	stopChan     chan struct{}
	cleanupDone  chan struct{}
	actorStopped sync.WaitGroup

	ctx context.Context
}

type writeRequest struct {
	sessions []*schema.Session
}

// propertyBuffer holds in-memory sessions for a single property.
type propertyBuffer struct {
	sessions []*schema.Session
	firstAdd time.Time
}

// NewBackgroundBatchingWriter creates a writer that uses spool file to queue the writes.
// Returns the writer, a cleanup function (to be deferred), and an error.
func NewBackgroundBatchingWriter(
	ctx context.Context,
	childWriter SessionWriter,
	opts ...Option,
) (SessionWriter, func(), error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// Ensure lvl2 directory exists
	if err := os.MkdirAll(cfg.lvl2Dir, 0o750); err != nil {
		return nil, nil, fmt.Errorf("failed to create spool directory: %w", err)
	}

	w := &backgroundBatchingWriter{
		childWriter:                      childWriter,
		lvl1MaxAge:                       cfg.lvl1MaxAge,
		lvl1MaxSessions:                  cfg.lvl1MaxSessions,
		lvl1SweepInterval:                cfg.lvl1SweepInterval,
		lvl2Dir:                          cfg.lvl2Dir,
		lvl2FlushInterval:                cfg.lvl2FlushInterval,
		encoder:                          cfg.encoder,
		decoder:                          cfg.decoder,
		maxConsecutiveChildWriteFailures: cfg.maxConsecutiveChildWriteFailures,
		writeChan:                        make(chan writeRequest, cfg.writeChanBuffer),
		flushChan:                        make(chan struct{}, 1),
		stopChan:                         make(chan struct{}),
		cleanupDone:                      make(chan struct{}),
		ctx:                              ctx,
	}

	w.actorStopped.Add(1)
	go w.actorLoop()

	cleanup := func() {
		close(w.stopChan)
		w.actorStopped.Wait()
		close(w.cleanupDone)
	}

	return w, cleanup, nil
}

// Write implements SessionWriter.
func (w *backgroundBatchingWriter) Write(sessions ...*schema.Session) error {
	if len(sessions) == 0 {
		return nil
	}

	req := writeRequest{sessions: sessions}
	select {
	case w.writeChan <- req:
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	case <-w.stopChan:
		return fmt.Errorf("writer is stopped")
	}
}

// actorLoop runs in a single goroutine and owns all state.
func (w *backgroundBatchingWriter) actorLoop() {
	defer w.actorStopped.Done()

	// Per-property lvl1 buffers
	lvl1Buffers := make(map[string]*propertyBuffer)

	// Consecutive failure counts per spool file path
	consecutiveFailuresBySpool := make(map[string]int)

	// Lvl1 sweep ticker
	lvl1Ticker := time.NewTicker(w.lvl1SweepInterval)
	defer lvl1Ticker.Stop()

	// Lvl2 flush ticker
	lvl2Ticker := time.NewTicker(w.lvl2FlushInterval)
	defer lvl2Ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			// Flush lvl1 to lvl2 (in-memory to disk) only
			// Do not flush lvl2 to child to avoid blocking on external warehouses
			w.flushLvl1ToLvl2(lvl1Buffers)
			return

		case req := <-w.writeChan:
			// Group sessions by property (should already be grouped by DirectCloser, but be safe)
			byProperty := make(map[string][]*schema.Session)
			for _, sess := range req.sessions {
				propID := sess.PropertyID
				if propID == "" {
					logrus.Warn("session has empty PropertyID, skipping")
					continue
				}
				byProperty[propID] = append(byProperty[propID], sess)
			}

			// Add to lvl1 buffers
			now := time.Now()
			for propID, sessions := range byProperty {
				buf, exists := lvl1Buffers[propID]
				if !exists {
					buf = &propertyBuffer{
						sessions: make([]*schema.Session, 0, w.lvl1MaxSessions),
						firstAdd: now,
					}
					lvl1Buffers[propID] = buf
				}
				buf.sessions = append(buf.sessions, sessions...)

				// Check if we should flush immediately (count threshold)
				if len(buf.sessions) >= w.lvl1MaxSessions {
					w.flushPropertyLvl1ToLvl2(propID, buf)
					delete(lvl1Buffers, propID)
				}
			}

		case <-lvl1Ticker.C:
			// Sweep: flush buffers that are too old
			now := time.Now()
			for propID, buf := range lvl1Buffers {
				if now.Sub(buf.firstAdd) >= w.lvl1MaxAge {
					w.flushPropertyLvl1ToLvl2(propID, buf)
					delete(lvl1Buffers, propID)
				}
			}

		case <-lvl2Ticker.C:
			// Periodic flush of lvl2 to child writer
			w.flushLvl2ToChild(consecutiveFailuresBySpool)
		}
	}
}

// flushPropertyLvl1ToLvl2 flushes a single property's lvl1 buffer to lvl2 spool file.
func (w *backgroundBatchingWriter) flushPropertyLvl1ToLvl2(propID string, buf *propertyBuffer) {
	if len(buf.sessions) == 0 {
		return
	}

	// Encode sessions to memory buffer
	var encodedBuf bytes.Buffer
	_, err := w.encoder(&encodedBuf, buf.sessions)
	if err != nil {
		logrus.Errorf("failed to encode sessions for property %q: %v", propID, err)
		return
	}

	payload := encodedBuf.Bytes()
	const maxUint32 = 0xFFFFFFFF
	if len(payload) > maxUint32 {
		logrus.Errorf("payload too large for property %q: %d bytes", propID, len(payload))
		return
	}
	payloadLen := uint32(len(payload)) //nolint:gosec // checked above

	// Get spool file path
	spoolPath := filepath.Join(w.lvl2Dir, fmt.Sprintf("property_%s.spool", propID))

	// Open file for append
	//nolint:gosec // path is constructed from property ID
	file, err := os.OpenFile(
		filepath.Clean(spoolPath),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0o644,
	)
	if err != nil {
		logrus.Errorf("failed to open spool file for property %q: %v", propID, err)
		return
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			logrus.Errorf("failed to close spool file for property %q: %v", propID, closeErr)
		}
	}()

	// Write header (4 bytes: payload length)
	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, payloadLen)
	if _, err := file.Write(header); err != nil {
		logrus.Errorf("failed to write header to spool file for property %q: %v", propID, err)
		return
	}

	// Write payload
	if _, err := file.Write(payload); err != nil {
		logrus.Errorf("failed to write payload to spool file for property %q: %v", propID, err)
		return
	}

	// Sync to ensure durability
	if err := file.Sync(); err != nil {
		logrus.Errorf("failed to sync spool file for property %q: %v", propID, err)
	}
}

// flushLvl1ToLvl2 flushes all lvl1 buffers to lvl2.
func (w *backgroundBatchingWriter) flushLvl1ToLvl2(buffers map[string]*propertyBuffer) {
	for propID, buf := range buffers {
		w.flushPropertyLvl1ToLvl2(propID, buf)
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
				// Clean up failure tracking for removed empty file
				delete(consecutiveFailuresBySpool, spoolPath)
			}
			continue
		}

		if err := w.childWriter.Write(allSessions...); err != nil {
			// Increment failure count for this spool file
			consecutiveFailuresBySpool[spoolPath]++
			failureCount := consecutiveFailuresBySpool[spoolPath]

			if failureCount >= w.maxConsecutiveChildWriteFailures {
				// Threshold exceeded: discard spool file
				logrus.Errorf(
					"discarding spool file %q after %d consecutive child writer failures (threshold: %d)",
					spoolPath,
					failureCount,
					w.maxConsecutiveChildWriteFailures,
				)
				if removeErr := os.Remove(spoolPath); removeErr != nil {
					logrus.Errorf("failed to remove discarded spool file %q: %v", spoolPath, removeErr)
				}
				delete(consecutiveFailuresBySpool, spoolPath)
				// Continue to next file (treat as success)
				continue
			}

			logrus.Errorf("failed to write sessions from spool file %q to child writer: %v", spoolPath, err)
			// Keep spool file on error (will retry on next flush)
			continue
		}

		logrus.Infof("wrote %d sessions from spool %q to warehouse", len(allSessions), spoolPath)

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
		return nil, fmt.Errorf("failed to open spool file: %w", err)
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
			// Clean end of file
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}
		if n != 4 {
			// Truncated header
			logrus.Warnf("truncated header in spool file %q, stopping read", path)
			break
		}

		payloadLen := binary.LittleEndian.Uint32(header)

		// Read payload
		payload := make([]byte, payloadLen)
		n, err = file.Read(payload)
		if err == io.EOF {
			// Truncated payload
			logrus.Warnf("truncated payload in spool file %q, stopping read", path)
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read payload: %w", err)
		}
		if n != int(payloadLen) {
			// Truncated payload
			logrus.Warnf("truncated payload in spool file %q (expected %d, got %d), stopping read", path, payloadLen, n)
			break
		}

		// Decode payload
		var sessions []*schema.Session
		if err := w.decoder(bytes.NewReader(payload), &sessions); err != nil {
			return nil, fmt.Errorf("failed to decode payload: %w", err)
		}

		allSessions = append(allSessions, sessions...)
	}

	return allSessions, nil
}

// config holds configuration for the writer.
type config struct {
	lvl1MaxAge                       time.Duration
	lvl1MaxSessions                  int
	lvl1SweepInterval                time.Duration
	lvl2Dir                          string
	lvl2FlushInterval                time.Duration
	encoder                          encoding.EncoderFunc
	decoder                          encoding.DecoderFunc
	maxConsecutiveChildWriteFailures int
	writeChanBuffer                  int
}

func defaultConfig() *config {
	return &config{
		lvl1MaxAge:                       5 * time.Second,
		lvl1MaxSessions:                  1000,
		lvl1SweepInterval:                250 * time.Millisecond,
		lvl2Dir:                          "/storage/writer",
		lvl2FlushInterval:                1 * time.Minute,
		encoder:                          encoding.GobEncoder,
		decoder:                          encoding.GobDecoder,
		maxConsecutiveChildWriteFailures: 20,
		writeChanBuffer:                  10000,
	}
}

// Option is a functional option for configuring the writer.
type Option func(*config)

// WithLvl1MaxAge sets the maximum age for lvl1 buffers.
func WithLvl1MaxAge(d time.Duration) Option {
	return func(c *config) {
		c.lvl1MaxAge = d
	}
}

// WithLvl1MaxSessions sets the maximum number of sessions in lvl1 buffer.
func WithLvl1MaxSessions(n int) Option {
	return func(c *config) {
		c.lvl1MaxSessions = n
	}
}

// WithLvl1SweepInterval sets the sweep interval for lvl1 buffers.
func WithLvl1SweepInterval(d time.Duration) Option {
	return func(c *config) {
		c.lvl1SweepInterval = d
	}
}

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
// child writer failures before a spool file is discarded.
func WithMaxConsecutiveChildWriteFailures(n int) Option {
	return func(c *config) {
		c.maxConsecutiveChildWriteFailures = n
	}
}

// WithWriteChanBuffer sets the capacity of the channel used for incoming Write calls.
// Larger values reduce blocking of callers when the actor is busy (e.g. during L2 flush)
// at the cost of more in-memory sessions on process crash. Zero means unbuffered.
// Default is 1000.
func WithWriteChanBuffer(n int) Option {
	return func(c *config) {
		c.writeChanBuffer = n
	}
}
