package sessions

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubSpool is a hand-written stub implementing spools.Spool for testing.
type stubSpool struct {
	mu       sync.Mutex
	data     map[string][][]byte // key -> list of appended payloads
	closed   bool
	appendFn func(key string, payload []byte) error
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

func (s *stubSpool) Flush(fn func(key string, inflightPath string, frames [][]byte) error) error {
	s.mu.Lock()
	snapshot := make(map[string][][]byte)
	for k, v := range s.data {
		snapshot[k] = v
		delete(s.data, k)
	}
	s.mu.Unlock()

	var flushErr error
	for key, frames := range snapshot {
		inflightPath := "/spool/" + key + ".spool.inflight"
		if err := fn(key, inflightPath, frames); err != nil {
			// Put back for retry on next flush.
			s.mu.Lock()
			s.data[key] = append(frames, s.data[key]...)
			s.mu.Unlock()
			flushErr = err
		}
	}
	return flushErr
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

// stubFailureStrategy records invocations for testing.
type stubFailureStrategy struct {
	mu    sync.Mutex
	paths []string
	err   error
}

func (s *stubFailureStrategy) OnExceededFailures(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paths = append(s.paths, path)
	return s.err
}

func (s *stubFailureStrategy) getPaths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.paths))
	copy(out, s.paths)
	return out
}

func TestPersistentSpoolWriter_WriteAndFlushRoundTrip(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{}
	strategy := &stubFailureStrategy{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child, strategy,
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
	strategy := &stubFailureStrategy{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child, strategy,
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
	strategy := &stubFailureStrategy{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child, strategy,
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

func TestPersistentSpoolWriter_FailureCountingAndStrategy(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{writeErr: fmt.Errorf("warehouse down")}
	strategy := &stubFailureStrategy{}
	maxFailures := 3

	w, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child, strategy,
		WithFlushInterval(1*time.Hour),
		WithMaxConsecutiveFailures(maxFailures),
	)
	require.NoError(t, err)
	defer cleanup()

	// Write some data
	err = w.Write(&schema.Session{PropertyID: "prop1"})
	require.NoError(t, err)

	// when — flush repeatedly to accumulate failures
	psw, ok := w.(*persistentSpoolWriter)
	require.True(t, ok)
	failuresByKey := make(map[string]int)
	for i := 0; i < maxFailures; i++ {
		psw.flush(failuresByKey)
	}

	// then — strategy should have been called once for the key
	paths := strategy.getPaths()
	require.Len(t, paths, 1)
	assert.Equal(t, "/spool/prop1.spool.inflight", paths[0])
}

func TestPersistentSpoolWriter_SuccessResetsFailureCount(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{writeErr: fmt.Errorf("temporary")}
	strategy := &stubFailureStrategy{}

	w, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child, strategy,
		WithFlushInterval(1*time.Hour),
		WithMaxConsecutiveFailures(5),
	)
	require.NoError(t, err)
	defer cleanup()

	err = w.Write(&schema.Session{PropertyID: "prop1"})
	require.NoError(t, err)

	psw, ok := w.(*persistentSpoolWriter)
	require.True(t, ok)
	failuresByKey := make(map[string]int)

	// Fail twice
	psw.flush(failuresByKey)
	psw.flush(failuresByKey)
	assert.Equal(t, 2, failuresByKey["prop1"])

	// Fix the child writer
	child.mu.Lock()
	child.writeErr = nil
	child.mu.Unlock()

	// Write more data and flush — should succeed and reset counter
	err = w.Write(&schema.Session{PropertyID: "prop1"})
	require.NoError(t, err)
	psw.flush(failuresByKey)
	assert.Equal(t, 0, failuresByKey["prop1"])
}

func TestPersistentSpoolWriter_RejectsNilDeps(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{}
	strategy := &stubFailureStrategy{}

	// when — nil spool
	_, _, errNilSpool := NewPersistentSpoolWriter(context.Background(), nil, child, strategy)

	// when — nil child
	_, _, errNilChild := NewPersistentSpoolWriter(context.Background(), spool, nil, strategy)

	// when — nil strategy
	_, _, errNilStrategy := NewPersistentSpoolWriter(context.Background(), spool, child, nil)

	// then
	require.Error(t, errNilSpool)
	assert.Contains(t, errNilSpool.Error(), "spool is required")

	require.Error(t, errNilChild)
	assert.Contains(t, errNilChild.Error(), "child writer is required")

	require.Error(t, errNilStrategy)
	assert.Contains(t, errNilStrategy.Error(), "failure strategy is required")
}

func TestPersistentSpoolWriter_WriteAfterClose(t *testing.T) {
	// given
	spool := newStubSpool()
	child := &stubChild{}
	strategy := &stubFailureStrategy{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child, strategy,
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
	strategy := &stubFailureStrategy{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child, strategy,
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
	strategy := &stubFailureStrategy{}

	writer, cleanup, err := NewPersistentSpoolWriter(
		context.Background(), spool, child, strategy,
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
