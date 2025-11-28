package sessions

import "github.com/d8a-tech/d8a/pkg/schema"

type noopWriter struct{}

var _ SessionWriter = (*noopWriter)(nil)

// Write implements SessionWriter.Write
func (w *noopWriter) Write(sessions ...*schema.Session) error {
	return nil
}

// NewNoopWriter creates a new NoopWriter
func NewNoopWriter() SessionWriter {
	return &noopWriter{}
}
