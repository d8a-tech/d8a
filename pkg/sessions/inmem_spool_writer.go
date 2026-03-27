package sessions

import (
	"fmt"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

type inMemSpoolWriter struct {
	child         SessionWriter
	maxSessions   int
	maxAge        time.Duration
	sweepInterval time.Duration
	writeChan     chan inMemWriteRequest
	stopped       chan struct{}
	mu            sync.RWMutex
	closed        bool
	closeOnce     sync.Once
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
	createdAt time.Time
}

func (w *inMemSpoolWriter) loop() {
	defer close(w.stopped)

	buffers := make(map[string]*propertyBuffer)
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
			w.bufferSessions(buffers, req.sessions)
			w.flushByCount(buffers)

		case <-ticker.C:
			w.flushByAge(buffers)
		}
	}
}

func (w *inMemSpoolWriter) bufferSessions(buffers map[string]*propertyBuffer, sessions []*schema.Session) {
	now := time.Now()
	for _, sess := range sessions {
		propID := sess.PropertyID
		if propID == "" {
			logrus.Warn("session has empty PropertyID, skipping")
			continue
		}
		buf, ok := buffers[propID]
		if !ok {
			buf = &propertyBuffer{createdAt: now}
			buffers[propID] = buf
		}
		buf.sessions = append(buf.sessions, sess)
	}
}

func (w *inMemSpoolWriter) flushByCount(buffers map[string]*propertyBuffer) {
	for propID, buf := range buffers {
		if len(buf.sessions) >= w.maxSessions {
			w.flushProperty(buffers, propID, buf)
		}
	}
}

func (w *inMemSpoolWriter) flushByAge(buffers map[string]*propertyBuffer) {
	now := time.Now()
	for propID, buf := range buffers {
		if now.Sub(buf.createdAt) >= w.maxAge {
			w.flushProperty(buffers, propID, buf)
		}
	}
}

func (w *inMemSpoolWriter) flushProperty(buffers map[string]*propertyBuffer, propID string, buf *propertyBuffer) {
	if err := w.child.Write(buf.sessions...); err != nil {
		logrus.Errorf("in-mem spool flush for property %q: %v", propID, err)
		// Keep sessions in the buffer for retry on the next sweep cycle.
		return
	}
	delete(buffers, propID)
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
