package sessions

import (
	"sync"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingWriter is a simple test double that records Write calls.
type recordingWriter struct {
	mu    sync.Mutex
	calls [][]*schema.Session
}

func (r *recordingWriter) Write(sessions ...*schema.Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]*schema.Session, len(sessions))
	copy(cp, sessions)
	r.calls = append(r.calls, cp)
	return nil
}

func (r *recordingWriter) allSessions() []*schema.Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	var all []*schema.Session
	for _, c := range r.calls {
		all = append(all, c...)
	}
	return all
}

func (r *recordingWriter) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recordingWriter) callsSnapshot() [][]*schema.Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]*schema.Session, len(r.calls))
	copy(out, r.calls)
	return out
}

func session(propID string) *schema.Session {
	return &schema.Session{PropertyID: propID}
}

func TestInMemSpoolWriter_FlushOnCount(t *testing.T) {
	// given
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(3),
		WithMaxAge(10*time.Minute), // large so age doesn't trigger
		WithSweepInterval(10*time.Minute),
	)
	require.NoError(t, err)
	defer cleanup()

	// when — write 3 sessions for property "A" (hits threshold)
	require.NoError(t, w.Write(session("A"), session("A"), session("A")))

	// then — wait a bit for goroutine to process
	assert.Eventually(t, func() bool {
		return child.callCount() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	all := child.allSessions()
	assert.Len(t, all, 3)
	for _, s := range all {
		assert.Equal(t, "A", s.PropertyID)
	}
}

func TestInMemSpoolWriter_FlushOnAge(t *testing.T) {
	// given
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1000), // large so count doesn't trigger
		WithMaxAge(50*time.Millisecond),
		WithSweepInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer cleanup()

	// when — write 1 session (below count threshold, but will age out)
	require.NoError(t, w.Write(session("B")))

	// then — sweep should flush after maxAge
	assert.Eventually(t, func() bool {
		return child.callCount() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	all := child.allSessions()
	assert.Len(t, all, 1)
	assert.Equal(t, "B", all[0].PropertyID)
}

func TestInMemSpoolWriter_DrainOnClose(t *testing.T) {
	// given
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1000),      // large so count doesn't trigger
		WithMaxAge(10*time.Minute), // large so age doesn't trigger
		WithSweepInterval(10*time.Minute),
	)
	require.NoError(t, err)

	// when — write sessions and immediately close
	require.NoError(t, w.Write(session("C"), session("C")))

	// Allow the write to be picked up by the loop before we close.
	time.Sleep(50 * time.Millisecond)

	cleanup()

	// then — all sessions should be drained to child
	all := child.allSessions()
	assert.Len(t, all, 2)
	for _, s := range all {
		assert.Equal(t, "C", s.PropertyID)
	}
}

func TestInMemSpoolWriter_PropertyIsolation(t *testing.T) {
	// given
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(2),
		WithMaxAge(10*time.Minute),
		WithSweepInterval(10*time.Minute),
	)
	require.NoError(t, err)
	defer cleanup()

	// when — write 2 sessions for "X" (triggers flush) and 1 for "Y" (does not)
	require.NoError(t, w.Write(session("X"), session("X"), session("Y")))

	// then — only "X" should flush by count
	assert.Eventually(t, func() bool {
		return child.callCount() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Give a short window to confirm no extra flush happens for "Y"
	time.Sleep(50 * time.Millisecond)

	calls := child.callsSnapshot()
	// Exactly one flush call for "X"
	assert.Len(t, calls, 1)
	assert.Len(t, calls[0], 2)
	for _, s := range calls[0] {
		assert.Equal(t, "X", s.PropertyID)
	}
}

func TestInMemSpoolWriter_WriteAfterClose(t *testing.T) {
	// given
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child)
	require.NoError(t, err)

	// when
	cleanup()
	err = w.Write(session("A"))

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stopped")
}

func TestInMemSpoolWriter_EmptyWriteIsNoop(t *testing.T) {
	// given
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child)
	require.NoError(t, err)
	defer cleanup()

	// when
	err = w.Write()

	// then
	assert.NoError(t, err)
	assert.Equal(t, 0, child.callCount())
}

func TestInMemSpoolWriter_NilChildReturnsError(t *testing.T) {
	// when
	_, _, err := NewInMemSpoolWriter(nil)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "child writer is required")
}

func TestInMemSpoolWriter_SkipsEmptyPropertyID(t *testing.T) {
	// given
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1),
	)
	require.NoError(t, err)
	defer cleanup()

	// when — write a session with empty PropertyID
	require.NoError(t, w.Write(&schema.Session{PropertyID: ""}))

	// Give a short window for any potential processing
	time.Sleep(50 * time.Millisecond)

	// then — nothing should be flushed
	assert.Equal(t, 0, child.callCount())
}
