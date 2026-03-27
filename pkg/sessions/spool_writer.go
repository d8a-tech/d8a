package sessions

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// SpoolWriter defines the interface for writing encoded session payloads to spool storage.
type SpoolWriter interface {
	Write(propID string, payload []byte) error
	Close() error
}

// SpoolFailureStrategy defines the action to take when a spool file exceeds
// the maximum number of consecutive child writer failures.
type SpoolFailureStrategy interface {
	OnExceededFailures(spoolPath string) error
}

const (
	spoolFilePrefix         = "property_"
	spoolFileSuffix         = ".spool"
	spoolInflightFileSuffix = ".spool.inflight"
)

func sanitizeSpoolPropertyID(propID string) string {
	return strings.NewReplacer("/", "_", "\\", "_").Replace(propID)
}

func activeSpoolFilename(propID string) string {
	return fmt.Sprintf("%s%s%s", spoolFilePrefix, sanitizeSpoolPropertyID(propID), spoolFileSuffix)
}

func activeSpoolPath(lvl2Dir, propID string) string {
	return filepath.Join(lvl2Dir, activeSpoolFilename(propID))
}

func inflightSpoolPathFromActivePath(activePath string) string {
	return activePath + ".inflight"
}

func activeSpoolPathFromInflightPath(inflightPath string) (string, bool) {
	if !strings.HasSuffix(inflightPath, spoolInflightFileSuffix) {
		return "", false
	}

	return strings.TrimSuffix(inflightPath, ".inflight"), true
}

func isActiveSpoolFileName(name string) bool {
	return strings.HasPrefix(name, spoolFilePrefix) && strings.HasSuffix(name, spoolFileSuffix)
}

func isInflightSpoolFileName(name string) bool {
	return strings.HasPrefix(name, spoolFilePrefix) && strings.HasSuffix(name, spoolInflightFileSuffix)
}

// bufferedSpoolWriter accumulates sessions in per-property in-memory buffers
// and flushes to lvl2 spool files on count/age thresholds.
type bufferedSpoolWriter struct {
	lvl2Dir           string
	lvl1MaxAge        time.Duration
	lvl1MaxSessions   int
	lvl1SweepInterval time.Duration

	writeChan chan bufferedWriteRequest
	stopChan  chan struct{}
	stopped   chan struct{}

	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

type bufferedWriteRequest struct {
	propID  string
	payload []byte
}

// NewBufferedSpoolWriter creates a SpoolWriter that buffers writes in memory
// and flushes to lvl2 spool files on count/age thresholds.
func NewBufferedSpoolWriter(
	lvl2Dir string,
	writeChanBuffer int,
	opts ...bufferedSpoolOption,
) (SpoolWriter, error) {
	if err := os.MkdirAll(lvl2Dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating spool directory: %w", err)
	}

	cfg := defaultBufferedConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	w := &bufferedSpoolWriter{
		lvl2Dir:           lvl2Dir,
		lvl1MaxAge:        cfg.lvl1MaxAge,
		lvl1MaxSessions:   cfg.lvl1MaxSessions,
		lvl1SweepInterval: cfg.lvl1SweepInterval,
		writeChan:         make(chan bufferedWriteRequest, writeChanBuffer),
		stopChan:          make(chan struct{}),
		stopped:           make(chan struct{}),
	}

	go w.loop()

	return w, nil
}

func (w *bufferedSpoolWriter) Write(propID string, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("buffered spool writer is stopped")
	}

	w.writeChan <- bufferedWriteRequest{propID: propID, payload: payload}
	return nil
}

// Close flushes all lvl1 buffers to lvl2 and stops the loop goroutine.
func (w *bufferedSpoolWriter) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
		close(w.stopChan)
		<-w.stopped
	})

	return nil
}

func (w *bufferedSpoolWriter) loop() {
	defer close(w.stopped)

	buffers := make(map[string]*payloadBuffer)

	ticker := time.NewTicker(w.lvl1SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			// Drain remaining items from writeChan before flushing
			w.drainWriteChan(buffers)
			w.flushAll(buffers)
			return

		case req := <-w.writeChan:
			buf, exists := buffers[req.propID]
			if !exists {
				buf = &payloadBuffer{
					payloads: make([][]byte, 0, w.lvl1MaxSessions),
					firstAdd: time.Now(),
				}
				buffers[req.propID] = buf
			}
			buf.payloads = append(buf.payloads, req.payload)

			if len(buf.payloads) >= w.lvl1MaxSessions {
				w.flushProperty(req.propID, buf)
				delete(buffers, req.propID)
			}

		case <-ticker.C:
			now := time.Now()
			for propID, buf := range buffers {
				if now.Sub(buf.firstAdd) >= w.lvl1MaxAge {
					w.flushProperty(propID, buf)
					delete(buffers, propID)
				}
			}
		}
	}
}

type payloadBuffer struct {
	payloads [][]byte
	firstAdd time.Time
}

func (w *bufferedSpoolWriter) drainWriteChan(buffers map[string]*payloadBuffer) {
	for {
		select {
		case req := <-w.writeChan:
			buf, exists := buffers[req.propID]
			if !exists {
				buf = &payloadBuffer{
					payloads: make([][]byte, 0, w.lvl1MaxSessions),
					firstAdd: time.Now(),
				}
				buffers[req.propID] = buf
			}
			buf.payloads = append(buf.payloads, req.payload)
		default:
			return
		}
	}
}

func (w *bufferedSpoolWriter) flushAll(buffers map[string]*payloadBuffer) {
	for propID, buf := range buffers {
		w.flushProperty(propID, buf)
	}
}

func (w *bufferedSpoolWriter) flushProperty(propID string, buf *payloadBuffer) {
	for _, payload := range buf.payloads {
		if err := appendFramedRecord(w.lvl2Dir, propID, payload); err != nil {
			logrus.Errorf("failed to flush buffered payload for property %q: %v", propID, err)
		}
	}
}

// appendFramedRecord writes a single framed record (4-byte LE length + payload)
// to the property spool file with O_APPEND and Sync.
func appendFramedRecord(lvl2Dir, propID string, payload []byte) error {
	const maxUint32 = 0xFFFFFFFF
	if len(payload) > maxUint32 {
		return fmt.Errorf("payload too large for property %q: %d bytes", propID, len(payload))
	}

	spoolPath := activeSpoolPath(lvl2Dir, propID)

	//nolint:gosec // path is constructed from property ID
	file, err := os.OpenFile(
		filepath.Clean(spoolPath),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0o644,
	)
	if err != nil {
		return fmt.Errorf("opening spool file for property %q: %w", propID, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			logrus.Errorf("failed to close spool file for property %q: %v", propID, closeErr)
		}
	}()

	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(payload))) //nolint:gosec // checked above
	if _, err := file.Write(header); err != nil {
		return fmt.Errorf("writing header for property %q: %w", propID, err)
	}
	if _, err := file.Write(payload); err != nil {
		return fmt.Errorf("writing payload for property %q: %w", propID, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing spool file for property %q: %w", propID, err)
	}

	return nil
}

// directSpoolWriter bypasses lvl1 and appends framed records directly to lvl2 spool files.
type directSpoolWriter struct {
	lvl2Dir string

	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

// NewDirectSpoolWriter creates a SpoolWriter that appends records directly to
// spool files with O_APPEND + Sync, returning only after durable write.
func NewDirectSpoolWriter(lvl2Dir string) (SpoolWriter, error) {
	if err := os.MkdirAll(lvl2Dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating spool directory: %w", err)
	}
	return &directSpoolWriter{lvl2Dir: lvl2Dir}, nil
}

// Write appends a framed record directly to the property spool file.
func (w *directSpoolWriter) Write(propID string, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("direct spool writer is stopped")
	}

	return appendFramedRecord(w.lvl2Dir, propID, payload)
}

// Close implements SpoolWriter.
func (w *directSpoolWriter) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
	})

	return nil
}

// deleteSpoolStrategy removes the spool file on exceeded failures (best_effort behavior).
type deleteSpoolStrategy struct{}

// NewDeleteSpoolStrategy creates a SpoolFailureStrategy that deletes spool files
// when consecutive child writer failures exceed the threshold.
func NewDeleteSpoolStrategy() SpoolFailureStrategy {
	return &deleteSpoolStrategy{}
}

// OnExceededFailures implements SpoolFailureStrategy.
func (s *deleteSpoolStrategy) OnExceededFailures(spoolPath string) error {
	if err := os.Remove(spoolPath); err != nil {
		return fmt.Errorf("removing discarded spool file %q: %w", spoolPath, err)
	}
	return nil
}

// quarantineSpoolStrategy renames the spool file to .quarantine on exceeded failures (at_least_once behavior).
type quarantineSpoolStrategy struct{}

// NewQuarantineSpoolStrategy creates a SpoolFailureStrategy that quarantines spool files
// by renaming them with a .quarantine suffix when consecutive child writer failures
// exceed the threshold.
func NewQuarantineSpoolStrategy() SpoolFailureStrategy {
	return &quarantineSpoolStrategy{}
}

// OnExceededFailures implements SpoolFailureStrategy.
func (s *quarantineSpoolStrategy) OnExceededFailures(spoolPath string) error {
	quarantinePath := spoolPath + ".quarantine"
	if err := os.Rename(spoolPath, quarantinePath); err != nil {
		return fmt.Errorf("quarantining spool file %q: %w", spoolPath, err)
	}
	logrus.Warnf("quarantined spool file %q to %q after exceeding failure threshold", spoolPath, quarantinePath)
	return nil
}

// bufferedSpoolConfig holds configuration for the buffered spool writer.
type bufferedSpoolConfig struct {
	lvl1MaxAge        time.Duration
	lvl1MaxSessions   int
	lvl1SweepInterval time.Duration
}

func defaultBufferedConfig() *bufferedSpoolConfig {
	return &bufferedSpoolConfig{
		lvl1MaxAge:        5 * time.Second,
		lvl1MaxSessions:   1000,
		lvl1SweepInterval: 250 * time.Millisecond,
	}
}

type bufferedSpoolOption func(*bufferedSpoolConfig)

// WithBufferedLvl1MaxAge sets the maximum age for lvl1 buffers.
func WithBufferedLvl1MaxAge(d time.Duration) bufferedSpoolOption {
	return func(c *bufferedSpoolConfig) {
		c.lvl1MaxAge = d
	}
}

// WithBufferedLvl1MaxSessions sets the maximum number of payloads in lvl1 buffer.
func WithBufferedLvl1MaxSessions(n int) bufferedSpoolOption {
	return func(c *bufferedSpoolConfig) {
		c.lvl1MaxSessions = n
	}
}

// WithBufferedLvl1SweepInterval sets the sweep interval for lvl1 buffers.
func WithBufferedLvl1SweepInterval(d time.Duration) bufferedSpoolOption {
	return func(c *bufferedSpoolConfig) {
		c.lvl1SweepInterval = d
	}
}
