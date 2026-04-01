package sessions

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/sirupsen/logrus"
)

type persistentSpoolWriter struct {
	child   SessionWriter
	spool   spools.Spool
	encoder encoding.EncoderFunc
	decoder encoding.DecoderFunc
}

// PersistentSpoolOption configures a persistentSpoolWriter.
type PersistentSpoolOption func(*persistentSpoolWriter)

// WithEncoderDecoder sets the encoder and decoder functions.
func WithEncoderDecoder(encoder encoding.EncoderFunc, decoder encoding.DecoderFunc) PersistentSpoolOption {
	return func(w *persistentSpoolWriter) {
		w.encoder = encoder
		w.decoder = decoder
	}
}

// NewPersistentSpoolWriter creates a SessionWriter decorator that encodes
// sessions and appends them to a Spool keyed by PropertyID.
func NewPersistentSpoolWriter(
	spoolFactory spools.Factory,
	child SessionWriter,
	opts ...PersistentSpoolOption,
) (SessionWriter, error) {
	if spoolFactory == nil {
		return nil, fmt.Errorf("spool factory is required")
	}
	if child == nil {
		return nil, fmt.Errorf("child writer is required")
	}

	w := &persistentSpoolWriter{
		child:   child,
		encoder: encoding.GobEncoder,
		decoder: encoding.GobDecoder,
	}
	for _, opt := range opts {
		opt(w)
	}

	spool, err := spoolFactory.Create(func(key string, next func() ([][]byte, error)) error {
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
	if err != nil {
		return nil, fmt.Errorf("creating spool: %w", err)
	}
	w.spool = spool

	return w, nil
}

// Write implements SessionWriter by grouping sessions per PropertyID,
// encoding each group, and appending to the spool.
func (w *persistentSpoolWriter) Write(sessions ...*schema.Session) error {
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
