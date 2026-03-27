package sessions

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/sirupsen/logrus"
)

type persistentSpoolWriter struct {
	child           SessionWriter
	spool           spools.Spool
	failureStrategy SpoolFailureStrategy
	encoder         encoding.EncoderFunc
	decoder         encoding.DecoderFunc
	flushInterval   time.Duration
	maxFailures     int

	stopChan     chan struct{}
	cleanupDone  chan struct{}
	actorStopped sync.WaitGroup
	ctx          context.Context
	mu           sync.RWMutex
	closed       bool
	closeOnce    sync.Once
}

// PersistentSpoolOption configures a persistentSpoolWriter.
type PersistentSpoolOption func(*persistentSpoolWriter)

// WithFlushInterval sets the background flush interval.
func WithFlushInterval(d time.Duration) PersistentSpoolOption {
	return func(w *persistentSpoolWriter) {
		w.flushInterval = d
	}
}

// WithEncoderDecoder sets the encoder and decoder functions.
func WithEncoderDecoder(encoder encoding.EncoderFunc, decoder encoding.DecoderFunc) PersistentSpoolOption {
	return func(w *persistentSpoolWriter) {
		w.encoder = encoder
		w.decoder = decoder
	}
}

// WithMaxConsecutiveFailures sets the maximum number of consecutive child
// writer failures before the failure strategy is invoked for a key.
func WithMaxConsecutiveFailures(n int) PersistentSpoolOption {
	return func(w *persistentSpoolWriter) {
		w.maxFailures = n
	}
}

// NewPersistentSpoolWriter creates a SessionWriter decorator that encodes
// sessions, appends them to a Spool keyed by PropertyID, and periodically
// flushes via a background actor loop that decodes and delegates to child.
// Returns the writer, a cleanup function, and an error.
func NewPersistentSpoolWriter(
	ctx context.Context,
	spool spools.Spool,
	child SessionWriter,
	failureStrategy SpoolFailureStrategy,
	opts ...PersistentSpoolOption,
) (SessionWriter, func(), error) {
	if spool == nil {
		return nil, nil, fmt.Errorf("spool is required")
	}
	if child == nil {
		return nil, nil, fmt.Errorf("child writer is required")
	}
	if failureStrategy == nil {
		return nil, nil, fmt.Errorf("failure strategy is required")
	}

	w := &persistentSpoolWriter{
		child:           child,
		spool:           spool,
		failureStrategy: failureStrategy,
		encoder:         encoding.GobEncoder,
		decoder:         encoding.GobDecoder,
		flushInterval:   1 * time.Minute,
		maxFailures:     20,
		stopChan:        make(chan struct{}),
		cleanupDone:     make(chan struct{}),
		ctx:             ctx,
	}
	for _, opt := range opts {
		opt(w)
	}

	w.actorStopped.Add(1)
	go w.actorLoop()

	cleanup := func() {
		w.closeOnce.Do(func() {
			w.mu.Lock()
			w.closed = true
			w.mu.Unlock()

			close(w.stopChan)
			w.actorStopped.Wait()
			close(w.cleanupDone)
		})
	}

	return w, cleanup, nil
}

// Write implements SessionWriter by grouping sessions per PropertyID,
// encoding each group, and appending to the spool.
func (w *persistentSpoolWriter) Write(sessions ...*schema.Session) error {
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return fmt.Errorf("writer is stopped")
	}
	w.mu.RUnlock()

	if len(sessions) == 0 {
		return nil
	}

	byProperty := make(map[string][]*schema.Session)
	for _, sess := range sessions {
		propID := sess.PropertyID
		if propID == "" {
			logrus.Warn("session has empty PropertyID, skipping")
			continue
		}
		byProperty[propID] = append(byProperty[propID], sess)
	}

	for propID, propertySessions := range byProperty {
		var buf bytes.Buffer
		if _, err := w.encoder(&buf, propertySessions); err != nil {
			return fmt.Errorf("encoding sessions for property %q: %w", propID, err)
		}
		if err := w.spool.Append(propID, buf.Bytes()); err != nil {
			return fmt.Errorf("appending to spool for property %q: %w", propID, err)
		}
	}

	return nil
}

func (w *persistentSpoolWriter) actorLoop() {
	defer w.actorStopped.Done()

	failuresByKey := make(map[string]int)

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			return
		case <-ticker.C:
			w.flush(failuresByKey)
		}
	}
}

func (w *persistentSpoolWriter) flush(failuresByKey map[string]int) {
	flushErr := w.spool.Flush(func(key string, inflightPath string, frames [][]byte) error {
		allSessions, decodeErr := w.decodeFrames(frames)
		if decodeErr != nil {
			return fmt.Errorf("decoding frames for key %q: %w", key, decodeErr)
		}

		if len(allSessions) == 0 {
			delete(failuresByKey, key)
			return nil
		}

		if writeErr := w.child.Write(allSessions...); writeErr != nil {
			failuresByKey[key]++
			count := failuresByKey[key]

			if count >= w.maxFailures {
				logrus.Errorf(
					"exceeded failure threshold for key %q after %d consecutive failures (threshold: %d)",
					key, count, w.maxFailures,
				)
				if stratErr := w.failureStrategy.OnExceededFailures(inflightPath); stratErr != nil {
					logrus.Errorf("failure strategy error for key %q: %v", key, stratErr)
					return fmt.Errorf("failure strategy for key %q: %w", key, stratErr)
				}
				delete(failuresByKey, key)
				// Return nil so Flush removes the inflight file (strategy already handled it).
				return nil
			}

			return fmt.Errorf("child write for key %q: %w", key, writeErr)
		}

		delete(failuresByKey, key)
		return nil
	})
	if flushErr != nil {
		logrus.Warnf("spool flush error: %v", flushErr)
	}
}

func (w *persistentSpoolWriter) decodeFrames(frames [][]byte) ([]*schema.Session, error) {
	var all []*schema.Session
	for _, frame := range frames {
		var sessions []*schema.Session
		if err := w.decoder(bytes.NewReader(frame), &sessions); err != nil {
			return nil, fmt.Errorf("decoding frame: %w", err)
		}
		all = append(all, sessions...)
	}
	return all, nil
}
