package sessions

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubSpool is a hand-written stub implementing spools.Spool for testing.
// When batchSize > 0, Flush yields frames in batches of that size via
// successive next() calls; otherwise all frames are returned in one batch.
type stubSpool struct {
	mu        sync.Mutex
	data      map[string][][]byte // key -> list of appended payloads
	closed    bool
	appendFn  func(key string, payload []byte) error
	batchSize int // 0 means single-batch (all frames at once)
}

func newStubSpool() *stubSpool {
	return &stubSpool{data: make(map[string][][]byte)}
}

func (s *stubSpool) Append(key string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appendFn != nil {
		return s.appendFn(key, payload)
	}
	s.data[key] = append(s.data[key], append([]byte(nil), payload...))
	return nil
}

func (s *stubSpool) Flush(fn func(key string, next func() ([][]byte, error)) error) error {
	s.mu.Lock()
	snapshot := make(map[string][][]byte)
	for k, v := range s.data {
		snapshot[k] = v
		delete(s.data, k)
	}
	batchSz := s.batchSize
	s.mu.Unlock()

	var flushErr error
	for key, frames := range snapshot {
		next := s.buildNext(frames, batchSz)

		if err := fn(key, next); err != nil {
			// Put back for retry on next flush.
			s.mu.Lock()
			s.data[key] = append(frames, s.data[key]...)
			s.mu.Unlock()
			flushErr = err
		}
	}
	return flushErr
}

// buildNext returns a next function that yields frames in batches of batchSz.
// When batchSz <= 0, all frames are returned in a single call.
func (s *stubSpool) buildNext(frames [][]byte, batchSz int) func() ([][]byte, error) {
	offset := 0
	if batchSz <= 0 {
		batchSz = len(frames)
	}
	return func() ([][]byte, error) {
		if offset >= len(frames) {
			return nil, io.EOF
		}
		end := offset + batchSz
		if end > len(frames) {
			end = len(frames)
		}
		batch := frames[offset:end]
		offset = end
		return batch, nil
	}
}

func (s *stubSpool) Recover() error { return nil }

func (s *stubSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *stubSpool) getFrames(key string) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.data[key]))
	copy(out, s.data[key])
	return out
}

// stubChild is a hand-written stub SessionWriter for testing.
type stubChild struct {
	mu       sync.Mutex
	calls    [][]*schema.Session
	writeErr error
}

func (c *stubChild) Write(sessions ...*schema.Session) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeErr != nil {
		return c.writeErr
	}
	copied := make([]*schema.Session, len(sessions))
	copy(copied, sessions)
	c.calls = append(c.calls, copied)
	return nil
}

func (c *stubChild) getCalls() [][]*schema.Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]*schema.Session, len(c.calls))
	copy(out, c.calls)
	return out
}

func TestPersistentSpoolWriter_WriteAndFlushRoundTrip(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child,
		WithFlushInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	defer cleanup()

	sessions := []*schema.Session{
		{PropertyID: "prop1"},
		{PropertyID: "prop1"},
		{PropertyID: "prop2"},
	}

	// when
	err = writer.Write(sessions...)
	require.NoError(t, err)

	// then — data should be in the spool, grouped by property
	prop1Frames := spool.getFrames("prop1")
	prop2Frames := spool.getFrames("prop2")
	assert.Len(t, prop1Frames, 1, "prop1 should have 1 encoded frame")
	assert.Len(t, prop2Frames, 1, "prop2 should have 1 encoded frame")

	// Wait for background flush to fire
	time.Sleep(200 * time.Millisecond)

	// then — child should have received the sessions
	calls := child.getCalls()
	require.NotEmpty(t, calls)

	var totalSessions int
	for _, c := range calls {
		totalSessions += len(c)
	}
	assert.Equal(t, 3, totalSessions)
}

func TestPersistentSpoolWriter_GroupsByProperty(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child,
		WithFlushInterval(1*time.Hour), // no auto-flush
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	err = writer.Write(
		&schema.Session{PropertyID: "A"},
		&schema.Session{PropertyID: "B"},
		&schema.Session{PropertyID: "A"},
	)
	require.NoError(t, err)

	// then — two keys in the spool
	framesA := spool.getFrames("A")
	framesB := spool.getFrames("B")
	assert.Len(t, framesA, 1)
	assert.Len(t, framesB, 1)

	// Decode frame A to verify grouping
	var decoded []*schema.Session
	err = encoding.GobDecoder(bytes.NewReader(framesA[0]), &decoded)
	require.NoError(t, err)
	assert.Len(t, decoded, 2)
}

func TestPersistentSpoolWriter_SkipsEmptyPropertyID(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child,
		WithFlushInterval(1*time.Hour),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	err = writer.Write(
		&schema.Session{PropertyID: ""},
		&schema.Session{PropertyID: "valid"},
	)
	require.NoError(t, err)

	// then — only "valid" in spool
	assert.Empty(t, spool.getFrames(""))
	assert.Len(t, spool.getFrames("valid"), 1)
}

func TestPersistentSpoolWriter_RejectsNilDeps(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{}

	// when — nil spool
	_, _, errNilSpool := NewPersistentSpoolWriter(context.Background(), nil, child)

	// when — nil child
	_, _, errNilChild := NewPersistentSpoolWriter(context.Background(), spool, nil)

	// then
	require.Error(t, errNilSpool)
	assert.Contains(t, errNilSpool.Error(), "spool is required")

	require.Error(t, errNilChild)
	assert.Contains(t, errNilChild.Error(), "child writer is required")
}

func TestPersistentSpoolWriter_WriteAfterClose(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child,
		WithFlushInterval(1*time.Hour),
	)
	require.NoError(t, err)

	// when
	cleanup()
	err = writer.Write(&schema.Session{PropertyID: "prop1"})

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stopped")
}

func TestPersistentSpoolWriter_EmptyWriteIsNoop(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child,
		WithFlushInterval(1*time.Hour),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	err = writer.Write()

	// then
	require.NoError(t, err)
	assert.Empty(t, spool.data)
}

func TestPersistentSpoolWriter_AppendError(t *testing.T) {
	// given
	spool := newStubSpool()
	spool.appendFn = func(_ string, _ []byte) error {
		return fmt.Errorf("disk full")
	}
	child := &stubChild{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child,
		WithFlushInterval(1*time.Hour),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	err = writer.Write(&schema.Session{PropertyID: "prop1"})

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "appending to spool")
}

func TestPersistentSpoolWriter_FlushDrainsMultipleBatches(t *testing.T) {
	// given — spool configured to yield one frame per next() call
	spool := newStubSpool()
	spool.batchSize = 1
	child := &stubChild{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child,
		WithFlushInterval(1*time.Hour), // no auto-flush
	)
	require.NoError(t, err)
	defer cleanup()

	// Append three separate Write calls so the spool stores three frames
	// under the same key. With batchSize=1, the next() function will be
	// called four times: three yielding one frame each, and a fourth
	// returning io.EOF.
	require.NoError(t, writer.Write(&schema.Session{PropertyID: "K", BrokenReason: "s1"}))
	require.NoError(t, writer.Write(&schema.Session{PropertyID: "K", BrokenReason: "s2"}))
	require.NoError(t, writer.Write(&schema.Session{PropertyID: "K", BrokenReason: "s3"}))

	assert.Len(t, spool.getFrames("K"), 3, "three frames before flush")

	// when — trigger a manual flush
	w, ok := writer.(*persistentSpoolWriter)
	require.True(t, ok)
	w.flush()

	// then — child receives exactly one Write call containing all three sessions
	calls := child.getCalls()
	require.Len(t, calls, 1, "single child.Write call for the inflight file")
	assert.Len(t, calls[0], 3, "all three sessions decoded across batches")

	// Verify session identity to confirm ordering is preserved
	assert.Equal(t, "s1", calls[0][0].BrokenReason)
	assert.Equal(t, "s2", calls[0][1].BrokenReason)
	assert.Equal(t, "s3", calls[0][2].BrokenReason)

	// Spool should be drained
	assert.Empty(t, spool.getFrames("K"))
}

func TestPersistentSpoolWriter_MultiBatchRetryPreservesFrames(t *testing.T) {
	// given — spool yields frames in batches of 2
	spool := newStubSpool()
	spool.batchSize = 2
	child := &stubChild{writeErr: fmt.Errorf("downstream failure")}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child,
		WithFlushInterval(1*time.Hour),
	)
	require.NoError(t, err)
	defer cleanup()

	require.NoError(t, writer.Write(&schema.Session{PropertyID: "R", BrokenReason: "a"}))
	require.NoError(t, writer.Write(&schema.Session{PropertyID: "R", BrokenReason: "b"}))
	require.NoError(t, writer.Write(&schema.Session{PropertyID: "R", BrokenReason: "c"}))

	// when — flush fails because child returns an error
	w, ok := writer.(*persistentSpoolWriter)
	require.True(t, ok)
	w.flush()

	// then — frames are put back for retry
	assert.Len(t, spool.getFrames("R"), 3, "all frames retained for retry")

	// when — clear the error and flush again
	child.mu.Lock()
	child.writeErr = nil
	child.mu.Unlock()
	w.flush()

	// then — child receives all sessions on retry
	calls := child.getCalls()
	require.Len(t, calls, 1)
	assert.Len(t, calls[0], 3)
}
