package receiver

import "github.com/d8a-tech/d8a/pkg/hits"

// memoryBatchingBackend is the default in-memory backend for BatchingStorage.
// It keeps staged hits in a slice and only clears them after a successful flush callback.
type memoryBatchingBackend struct {
	buffer []*hits.Hit
}

// Append implements BatchingBackend.
func (m *memoryBatchingBackend) Append(h []*hits.Hit) error {
	m.buffer = append(m.buffer, h...)
	return nil
}

// Flush implements BatchingBackend.
func (m *memoryBatchingBackend) Flush(cb func([]*hits.Hit) error) error {
	if len(m.buffer) == 0 {
		return nil
	}

	toSend := make([]*hits.Hit, len(m.buffer))
	copy(toSend, m.buffer)

	if err := cb(toSend); err != nil {
		return err
	}

	m.buffer = m.buffer[:0]
	return nil
}

// Close implements BatchingBackend.
func (m *memoryBatchingBackend) Close() error {
	return nil
}
