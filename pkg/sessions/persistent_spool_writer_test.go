package sessions

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubSpool struct {
	mu       sync.Mutex
	data     map[string][][]byte
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

func (s *stubSpool) getFrames(key string) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([][]byte, len(s.data[key]))
	copy(out, s.data[key])
	return out
}

type stubFactory struct {
	mu      sync.Mutex
	spool   spools.Spool
	err     error
	handler spools.FlushHandler
}

func newStubFactory(spool spools.Spool) *stubFactory {
	return &stubFactory{spool: spool}
}

func (f *stubFactory) Create(handler spools.FlushHandler) (spools.Spool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.handler = handler
	if f.err != nil {
		return nil, f.err
	}
	return f.spool, nil
}

func (f *stubFactory) Close() error {
	return nil
}

func (f *stubFactory) getHandler() spools.FlushHandler {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handler
}

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

func TestPersistentSpoolWriter_WriteAndFlushHandlerRoundTrip(t *testing.T) {
	// given
	spool := newStubSpool()
	factory := newStubFactory(spool)
	child := &stubChild{}

	writer, err := NewPersistentSpoolWriter(factory, child)
	require.NoError(t, err)

	err = writer.Write(
		&schema.Session{PropertyID: "A", BrokenReason: "s1"},
		&schema.Session{PropertyID: "A", BrokenReason: "s2"},
		&schema.Session{PropertyID: "B", BrokenReason: "s3"},
	)
	require.NoError(t, err)

	framesA := spool.getFrames("A")
	framesB := spool.getFrames("B")
	require.Len(t, framesA, 1)
	require.Len(t, framesB, 1)

	handler := factory.getHandler()
	require.NotNil(t, handler)

	nextA := func() ([][]byte, error) {
		if len(framesA) == 0 {
			return nil, io.EOF
		}
		batch := framesA
		framesA = nil
		return batch, nil
	}
	nextB := func() ([][]byte, error) {
		if len(framesB) == 0 {
			return nil, io.EOF
		}
		batch := framesB
		framesB = nil
		return batch, nil
	}

	// when
	require.NoError(t, handler("A", nextA))
	require.NoError(t, handler("B", nextB))

	// then
	calls := child.getCalls()
	require.Len(t, calls, 2)
	assert.Equal(t, "s1", calls[0][0].BrokenReason)
	assert.Equal(t, "s2", calls[0][1].BrokenReason)
	assert.Equal(t, "s3", calls[1][0].BrokenReason)
}

func TestPersistentSpoolWriter_GroupsByProperty(t *testing.T) {
	// given
	spool := newStubSpool()
	factory := newStubFactory(spool)
	child := &stubChild{}

	writer, err := NewPersistentSpoolWriter(factory, child)
	require.NoError(t, err)

	// when
	err = writer.Write(
		&schema.Session{PropertyID: "A"},
		&schema.Session{PropertyID: "B"},
		&schema.Session{PropertyID: "A"},
	)
	require.NoError(t, err)

	// then
	framesA := spool.getFrames("A")
	framesB := spool.getFrames("B")
	assert.Len(t, framesA, 1)
	assert.Len(t, framesB, 1)

	var decoded []*schema.Session
	err = encoding.GobDecoder(bytes.NewReader(framesA[0]), &decoded)
	require.NoError(t, err)
	assert.Len(t, decoded, 2)
}

func TestPersistentSpoolWriter_SkipsEmptyPropertyID(t *testing.T) {
	// given
	spool := newStubSpool()
	factory := newStubFactory(spool)
	child := &stubChild{}

	writer, err := NewPersistentSpoolWriter(factory, child)
	require.NoError(t, err)

	// when
	err = writer.Write(
		&schema.Session{PropertyID: ""},
		&schema.Session{PropertyID: "valid"},
	)
	require.NoError(t, err)

	// then
	assert.Empty(t, spool.getFrames(""))
	assert.Len(t, spool.getFrames("valid"), 1)
}

func TestPersistentSpoolWriter_RejectsNilDeps(t *testing.T) {
	// given
	spool := newStubSpool()
	factory := newStubFactory(spool)
	child := &stubChild{}

	// when
	_, errNilFactory := NewPersistentSpoolWriter(nil, child)
	_, errNilChild := NewPersistentSpoolWriter(factory, nil)

	// then
	require.Error(t, errNilFactory)
	assert.Contains(t, errNilFactory.Error(), "spool factory is required")

	require.Error(t, errNilChild)
	assert.Contains(t, errNilChild.Error(), "child writer is required")
}

func TestPersistentSpoolWriter_EmptyWriteIsNoop(t *testing.T) {
	// given
	spool := newStubSpool()
	factory := newStubFactory(spool)
	child := &stubChild{}

	writer, err := NewPersistentSpoolWriter(factory, child)
	require.NoError(t, err)

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
	factory := newStubFactory(spool)
	child := &stubChild{}

	writer, err := NewPersistentSpoolWriter(factory, child)
	require.NoError(t, err)

	// when
	err = writer.Write(&schema.Session{PropertyID: "prop1"})

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "appending to spool")
}

func TestPersistentSpoolWriter_CreateError(t *testing.T) {
	// given
	spool := newStubSpool()
	factory := newStubFactory(spool)
	factory.err = fmt.Errorf("factory failure")
	child := &stubChild{}

	// when
	_, err := NewPersistentSpoolWriter(factory, child)

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating spool")
}

func TestPersistentSpoolWriter_FlushHandlerDrainsMultipleBatches(t *testing.T) {
	// given
	spool := newStubSpool()
	factory := newStubFactory(spool)
	child := &stubChild{}

	writer, err := NewPersistentSpoolWriter(factory, child)
	require.NoError(t, err)

	require.NoError(t, writer.Write(&schema.Session{PropertyID: "K", BrokenReason: "s1"}))
	require.NoError(t, writer.Write(&schema.Session{PropertyID: "K", BrokenReason: "s2"}))
	require.NoError(t, writer.Write(&schema.Session{PropertyID: "K", BrokenReason: "s3"}))

	frames := spool.getFrames("K")
	require.Len(t, frames, 3)
	handler := factory.getHandler()
	require.NotNil(t, handler)

	offset := 0
	next := func() ([][]byte, error) {
		if offset >= len(frames) {
			return nil, io.EOF
		}
		end := offset + 1
		batch := frames[offset:end]
		offset = end
		return batch, nil
	}

	// when
	require.NoError(t, handler("K", next))

	// then
	calls := child.getCalls()
	require.Len(t, calls, 1)
	assert.Len(t, calls[0], 3)
	assert.Equal(t, "s1", calls[0][0].BrokenReason)
	assert.Equal(t, "s2", calls[0][1].BrokenReason)
	assert.Equal(t, "s3", calls[0][2].BrokenReason)
}

func TestPersistentSpoolWriter_FlushHandlerWriteError(t *testing.T) {
	// given
	spool := newStubSpool()
	factory := newStubFactory(spool)
	child := &stubChild{writeErr: fmt.Errorf("downstream failure")}

	writer, err := NewPersistentSpoolWriter(factory, child)
	require.NoError(t, err)

	require.NoError(t, writer.Write(&schema.Session{PropertyID: "R"}))
	frames := spool.getFrames("R")
	handler := factory.getHandler()
	require.NotNil(t, handler)

	next := func() ([][]byte, error) {
		if len(frames) == 0 {
			return nil, io.EOF
		}
		batch := frames
		frames = nil
		return batch, nil
	}

	// when
	err = handler("R", next)

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "child write for key")
}
