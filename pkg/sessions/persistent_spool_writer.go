package sessions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/sirupsen/logrus"
)

type persistentSpoolWriter struct {
	child         SessionWriter
	spool         spools.Spool
	encoder       encoding.EncoderFunc
	decoder       encoding.DecoderFunc
	flushInterval time.Duration

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

// NewPersistentSpoolWriter creates a SessionWriter decorator that encodes
// sessions, appends them to a Spool keyed by PropertyID, and periodically
// flushes via a background actor loop that decodes and delegates to child.
// Returns the writer, a cleanup function, and an error.
func NewPersistentSpoolWriter(
	ctx context.Context,
	spool spools.Spool,
	child SessionWriter,
	opts ...PersistentSpoolOption,
) (SessionWriter, func(), error) {
	if spool == nil {
		return nil, nil, fmt.Errorf("spool is required")
	}
	if child == nil {
		return nil, nil, fmt.Errorf("child writer is required")
	}

	w := &persistentSpoolWriter{
		child:         child,
		spool:         spool,
		encoder:       encoding.GobEncoder,
		decoder:       encoding.GobDecoder,
		flushInterval: 1 * time.Minute,
		stopChan:      make(chan struct{}),
		cleanupDone:   make(chan struct{}),
		ctx:           ctx,
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

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			// Appended data is assumed durable enough in the spool; no final flush here.
			return
		case <-ticker.C:
			w.flush()
		}
	}
}

func (w *persistentSpoolWriter) flush() {
	flushErr := w.spool.Flush(func(key string, next func() ([][]byte, error)) error {
		allSessions, decodeErr := w.drainAndDecode(next)
		if decodeErr != nil {
			return fmt.Errorf("decoding frames for key %q: %w", key, decodeErr)
		}

		if len(allSessions) == 0 {
			return nil
		}

		if writeErr := w.child.Write(allSessions...); writeErr != nil {
			return fmt.Errorf("child write for key %q: %w", key, writeErr)
		}

		return nil
	})
	if flushErr != nil {
		logrus.Warnf("spool flush error: %v", flushErr)
	}
}

// drainAndDecode repeatedly calls next to obtain frame batches until io.EOF,
// decoding each batch into sessions and concatenating the results.
func (w *persistentSpoolWriter) drainAndDecode(next func() ([][]byte, error)) ([]*schema.Session, error) {
	var all []*schema.Session
	for {
		frames, err := next()
		if errors.Is(err, io.EOF) {
			return all, nil
		}
		if err != nil {
			return nil, fmt.Errorf("reading next batch: %w", err)
		}
		for _, frame := range frames {
			var sessions []*schema.Session
			if decErr := w.decoder(bytes.NewReader(frame), &sessions); decErr != nil {
				return nil, fmt.Errorf("decoding frame: %w", decErr)
			}
			all = append(all, sessions...)
		}
	}
}
