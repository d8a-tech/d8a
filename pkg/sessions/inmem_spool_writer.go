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
	stopChan      chan struct{}
	stopped       chan struct{}
	mu            sync.Mutex
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
		stopChan:      make(chan struct{}),
		stopped:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(w)
	}

	go w.loop()

	cleanup := func() {
		w.closeOnce.Do(func() {
			w.mu.Lock()
			w.closed = true
			w.mu.Unlock()

			close(w.stopChan)
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

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("writer is stopped")
	}
	w.mu.Unlock()

	// Use select so we never block if the loop has already exited
	// (e.g. cleanup raced between the closed check above and this send).
	select {
	case w.writeChan <- inMemWriteRequest{sessions: sessions}:
		return nil
	case <-w.stopped:
		return fmt.Errorf("writer is stopped")
	}
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
		case req := <-w.writeChan:
			w.bufferSessions(buffers, req.sessions)
			w.flushByCount(buffers)

		case <-ticker.C:
			w.flushByAge(buffers)

		case <-w.stopChan:
			w.drain(buffers)
			return
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

func (w *inMemSpoolWriter) drain(buffers map[string]*propertyBuffer) {
	// Drain any remaining items on writeChan.
	for {
		select {
		case req := <-w.writeChan:
			w.bufferSessions(buffers, req.sessions)
		default:
			goto flushed
		}
	}
flushed:
	// Best-effort flush of all remaining buffers during shutdown.
	// On failure, log and discard — there is no next sweep cycle.
	for propID, buf := range buffers {
		if err := w.child.Write(buf.sessions...); err != nil {
			logrus.Errorf("in-mem spool drain flush for property %q: %v (sessions lost)", propID, err)
		}
		delete(buffers, propID)
	}
}
