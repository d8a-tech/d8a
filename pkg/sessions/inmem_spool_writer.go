package sessions

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

type inMemSpoolWriter struct {
	child          SessionWriter
	maxSessions    int
	maxAge         time.Duration
	sweepInterval  time.Duration
	maxBufferBytes int64
	writeChan      chan inMemWriteRequest
	stopped        chan struct{}
	mu             sync.RWMutex
	closed         bool
	closeOnce      sync.Once
}

type inMemWriteRequest struct {
	sessions []*schema.Session
}

// InMemSpoolOption configures an inMemSpoolWriter.
type InMemSpoolOption func(*inMemSpoolWriter)

// WithMaxSessions sets the per-property session count that triggers a flush.
func WithMaxSessions(n int) InMemSpoolOption {
	return func(w *inMemSpoolWriter) {
		w.maxSessions = n
	}
}

// WithMaxAge sets the maximum age a buffered session can have before
// a sweep flushes its property buffer.
func WithMaxAge(d time.Duration) InMemSpoolOption {
	return func(w *inMemSpoolWriter) {
		w.maxAge = d
	}
}

// WithSweepInterval sets how often the background loop checks for aged buffers.
func WithSweepInterval(d time.Duration) InMemSpoolOption {
	return func(w *inMemSpoolWriter) {
		w.sweepInterval = d
	}
}

// WithMaxBufferBytes sets the maximum total buffered size in bytes across all
// property buffers. When the limit is reached, incoming sessions are discarded
// with an error log. Zero (the default) means unlimited.
func WithMaxBufferBytes(n int64) InMemSpoolOption {
	return func(w *inMemSpoolWriter) {
		w.maxBufferBytes = n
	}
}

// NewInMemSpoolWriter creates a SessionWriter decorator that accumulates
// *schema.Session objects per property in memory and flushes to child on
// count or age thresholds. Returns the writer, a cleanup function, and an error.
func NewInMemSpoolWriter(child SessionWriter, opts ...InMemSpoolOption) (SessionWriter, func(), error) {
	if child == nil {
		return nil, nil, fmt.Errorf("child writer is required")
	}

	w := &inMemSpoolWriter{
		child:         child,
		maxSessions:   100,
		maxAge:        30 * time.Second,
		sweepInterval: 5 * time.Second,
		writeChan:     make(chan inMemWriteRequest, 256),
		stopped:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(w)
	}

	go w.loop()

	cleanup := func() {
		w.closeOnce.Do(func() {
			// Write-lock ensures no Write call is between its closed
			// check and its channel send. After this block, writeChan
			// is closed so the loop can range-drain it deterministically.
			w.mu.Lock()
			w.closed = true
			close(w.writeChan)
			w.mu.Unlock()

			<-w.stopped
		})
	}

	return w, cleanup, nil
}

// Write implements SessionWriter.
func (w *inMemSpoolWriter) Write(sessions ...*schema.Session) error {
	if len(sessions) == 0 {
		return nil
	}

	// Hold the read lock across the closed check and channel send so
	// cleanup cannot close writeChan between the two operations.
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed {
		return fmt.Errorf("writer is stopped")
	}

	w.writeChan <- inMemWriteRequest{sessions: sessions}
	return nil
}

type propertyBuffer struct {
	sessions  []*schema.Session
	sizeBytes int64
	createdAt time.Time
}

func (w *inMemSpoolWriter) loop() {
	defer close(w.stopped)

	buffers := make(map[string]*propertyBuffer)
	var totalBytes int64
	ticker := time.NewTicker(w.sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case req, ok := <-w.writeChan:
			if !ok {
				// Channel closed by cleanup — drain buffers and exit.
				w.flushAll(buffers)
				return
			}
			totalBytes = w.bufferSessions(buffers, req.sessions, totalBytes)
			totalBytes = w.flushByCount(buffers, totalBytes)

		case <-ticker.C:
			totalBytes = w.flushByAge(buffers, totalBytes)
		}
	}
}

func (w *inMemSpoolWriter) bufferSessions(
	buffers map[string]*propertyBuffer, sessions []*schema.Session, totalBytes int64,
) int64 {
	now := time.Now()
	for _, sess := range sessions {
		propID := sess.PropertyID
		if propID == "" {
			logrus.Warn("session has empty PropertyID, skipping")
			continue
		}

		sessBytes := estimateSessionBytes(sess)
		if w.maxBufferBytes > 0 && totalBytes+sessBytes > w.maxBufferBytes {
			logrus.Errorf("in-mem spool buffer limit reached (%d/%d bytes), discarding session for property %q",
				totalBytes, w.maxBufferBytes, propID)
			continue
		}

		buf, ok := buffers[propID]
		if !ok {
			buf = &propertyBuffer{createdAt: now}
			buffers[propID] = buf
		}
		buf.sessions = append(buf.sessions, sess)
		buf.sizeBytes += sessBytes
		totalBytes += sessBytes
	}
	return totalBytes
}

func (w *inMemSpoolWriter) flushByCount(buffers map[string]*propertyBuffer, totalBytes int64) int64 {
	for propID, buf := range buffers {
		if len(buf.sessions) >= w.maxSessions {
			totalBytes = w.flushProperty(buffers, propID, buf, totalBytes)
		}
	}
	return totalBytes
}

func (w *inMemSpoolWriter) flushByAge(buffers map[string]*propertyBuffer, totalBytes int64) int64 {
	now := time.Now()
	for propID, buf := range buffers {
		if now.Sub(buf.createdAt) >= w.maxAge {
			totalBytes = w.flushProperty(buffers, propID, buf, totalBytes)
		}
	}
	return totalBytes
}

func (w *inMemSpoolWriter) flushProperty(
	buffers map[string]*propertyBuffer, propID string, buf *propertyBuffer, totalBytes int64,
) int64 {
	if err := w.child.Write(buf.sessions...); err != nil {
		logrus.Errorf("in-mem spool flush for property %q: %v", propID, err)
		// Keep sessions in the buffer for retry on the next sweep cycle.
		return totalBytes
	}
	totalBytes -= buf.sizeBytes
	delete(buffers, propID)
	return totalBytes
}

func (w *inMemSpoolWriter) flushAll(buffers map[string]*propertyBuffer) {
	// Best-effort flush of all remaining buffers during shutdown.
	// On failure, log and discard — there is no next sweep cycle.
	for propID, buf := range buffers {
		if err := w.child.Write(buf.sessions...); err != nil {
			logrus.Errorf("in-mem spool drain flush for property %q: %v (sessions lost)", propID, err)
		}
		delete(buffers, propID)
	}
}

// estimateSessionBytes returns an approximate byte cost of a session for
// buffer-limit accounting. It uses Hit.Size() for each event's bound hit
// and adds fixed overhead for the Session/Event structs, slice headers,
// and map shells. The estimate is deliberately conservative (slightly
// over-counting) to prevent OOM rather than be perfectly precise.
func estimateSessionBytes(sess *schema.Session) int64 {
	if sess == nil {
		return 0
	}

	// Session struct overhead: struct shell + PropertyID string data +
	// BrokenReason string data + slice header + two map headers.
	const sessionOverhead = int64(unsafe.Sizeof(schema.Session{}))
	var size int64
	size += sessionOverhead
	size += int64(len(sess.PropertyID))
	size += int64(len(sess.BrokenReason))

	for _, ev := range sess.Events {
		// Event struct overhead.
		size += int64(unsafe.Sizeof(schema.Event{}))
		size += int64(len(ev.BrokenReason))

		if ev.BoundHit != nil {
			size += int64(ev.BoundHit.Size())
		}
	}

	return size
}
