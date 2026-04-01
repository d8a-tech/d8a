package sessions

import (
	"fmt"
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

func sessionWithEvents(propID string, numEvents int) *schema.Session {
	events := make([]*schema.Event, numEvents)
	for i := range events {
		events[i] = &schema.Event{}
	}
	return &schema.Session{PropertyID: propID, Events: events}
}

// failingWriter is a test double that fails a configurable number of times
// then succeeds, recording all successful writes.
type failingWriter struct {
	mu          sync.Mutex
	failCount   int // remaining failures
	calls       [][]*schema.Session
	failedCalls int
}

func (f *failingWriter) Write(sessions ...*schema.Session) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCount > 0 {
		f.failCount--
		f.failedCalls++
		return fmt.Errorf("simulated failure")
	}
	cp := make([]*schema.Session, len(sessions))
	copy(cp, sessions)
	f.calls = append(f.calls, cp)
	return nil
}

func (f *failingWriter) allSessions() []*schema.Session {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []*schema.Session
	for _, c := range f.calls {
		all = append(all, c...)
	}
	return all
}

func (f *failingWriter) getFailedCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failedCalls
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

func TestInMemSpoolWriter_WithWriteChanBuffer_ConfiguresChannelCapacity(t *testing.T) {
	// given
	child := &recordingWriter{}

	// when
	w, cleanup, err := NewInMemSpoolWriter(child, WithWriteChanBuffer(17))
	require.NoError(t, err)
	defer cleanup()

	// then
	concreteWriter, ok := w.(*inMemSpoolWriter)
	require.True(t, ok)
	assert.Equal(t, 17, cap(concreteWriter.writeChan))
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

func TestInMemSpoolWriter_RetainsBufferOnChildFailure(t *testing.T) {
	// given — child fails once then succeeds
	child := &failingWriter{failCount: 1}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1000),
		WithMaxAge(30*time.Millisecond),
		WithSweepInterval(15*time.Millisecond),
	)
	require.NoError(t, err)
	defer cleanup()

	// when — write a session; first sweep will fail, second should succeed
	require.NoError(t, w.Write(session("R")))

	// then — eventually child receives the session after retry
	assert.Eventually(t, func() bool {
		return len(child.allSessions()) >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// The child must have been called at least twice (one failure + one success)
	assert.GreaterOrEqual(t, child.getFailedCalls(), 1)
	all := child.allSessions()
	assert.Len(t, all, 1)
	assert.Equal(t, "R", all[0].PropertyID)
}

func TestInMemSpoolWriter_WriteDuringCleanupDoesNotBlock(t *testing.T) {
	// given
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1000),
		WithMaxAge(10*time.Minute),
		WithSweepInterval(10*time.Minute),
	)
	require.NoError(t, err)

	// when — start cleanup in background, then try to Write concurrently
	// The Write must not block forever even if it races with cleanup.
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Tight loop of writes — some will succeed, some will see "stopped".
		// The key property: none of them block forever.
		for i := 0; i < 100; i++ {
			_ = w.Write(session("D"))
		}
	}()

	cleanup()

	select {
	case <-done:
		// success — writes completed without blocking
	case <-time.After(2 * time.Second):
		t.Fatal("Write blocked forever during cleanup")
	}
}

func TestInMemSpoolWriter_RacingWriteIsDeliveredOrRejected(t *testing.T) {
	// This test verifies that every Write that returns nil has its sessions
	// delivered to child during drain, and every Write that is rejected
	// returns an error. No session may be silently lost.

	// given
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1000),      // high so count-flush doesn't trigger
		WithMaxAge(10*time.Minute), // high so age-flush doesn't trigger
		WithSweepInterval(10*time.Minute),
	)
	require.NoError(t, err)

	const numWriters = 20
	const writesPerWriter = 50

	var wg sync.WaitGroup
	var acceptedMu sync.Mutex
	accepted := 0

	// when — launch many concurrent writers, then trigger cleanup mid-flight
	wg.Add(numWriters)
	for g := 0; g < numWriters; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				if writeErr := w.Write(session("P")); writeErr == nil {
					acceptedMu.Lock()
					accepted++
					acceptedMu.Unlock()
				}
			}
		}()
	}

	// Give writers a head start, then clean up while they're still running.
	time.Sleep(5 * time.Millisecond)
	cleanup()
	wg.Wait()

	// then — child must have received exactly the number of accepted sessions
	all := child.allSessions()
	acceptedMu.Lock()
	expectedCount := accepted
	acceptedMu.Unlock()

	assert.Equal(t, expectedCount, len(all),
		"every accepted Write (returned nil) must be drained to child; got %d delivered vs %d accepted",
		len(all), expectedCount)
}

func TestInMemSpoolWriter_DiscardsSessionsWhenSessionLimitReached(t *testing.T) {
	// given — allow only 2 buffered sessions total
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1000),
		WithMaxAge(10*time.Minute),
		WithSweepInterval(10*time.Minute),
		WithMaxBufferedSessions(2),
		WithMaxBufferEvents(0), // unlimited events
	)
	require.NoError(t, err)

	// when — write 5 sessions (only 2 should be kept)
	require.NoError(t, w.Write(
		session("A"), session("A"), session("A"), session("A"), session("A"),
	))

	// Allow the write to be picked up by the loop.
	time.Sleep(50 * time.Millisecond)

	// then — close and check that only 2 sessions were buffered
	cleanup()
	all := child.allSessions()
	assert.Len(t, all, 2, "only sessions within the session count limit should be kept")
}

func TestInMemSpoolWriter_DiscardsSessionsWhenEventLimitReached(t *testing.T) {
	// given — allow only 3 total buffered events
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1000),
		WithMaxAge(10*time.Minute),
		WithSweepInterval(10*time.Minute),
		WithMaxBufferedSessions(0), // unlimited sessions
		WithMaxBufferEvents(3),
	)
	require.NoError(t, err)

	// when — write sessions with events: 2 events + 2 events (second would exceed limit)
	require.NoError(t, w.Write(
		sessionWithEvents("A", 2),
		sessionWithEvents("A", 2),
	))

	// Allow the write to be picked up by the loop.
	time.Sleep(50 * time.Millisecond)

	// then — only the first session (2 events) fits; second (2+2=4 > 3) is discarded
	cleanup()
	all := child.allSessions()
	assert.Len(t, all, 1, "second session should be discarded because total events would exceed limit")
}

func TestInMemSpoolWriter_BufferReclaimedAfterFlush(t *testing.T) {
	// given — session limit allows 2; flush by count at 2
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(2),
		WithMaxAge(10*time.Minute),
		WithSweepInterval(10*time.Minute),
		WithMaxBufferedSessions(2),
		WithMaxBufferEvents(0),
	)
	require.NoError(t, err)
	defer cleanup()

	// when — write 2 sessions (triggers flush, reclaims space)
	require.NoError(t, w.Write(session("A"), session("A")))

	// Wait for flush to happen
	assert.Eventually(t, func() bool {
		return child.callCount() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Write 2 more — should succeed because buffer was reclaimed
	require.NoError(t, w.Write(session("A"), session("A")))

	// Wait for second flush
	assert.Eventually(t, func() bool {
		return child.callCount() >= 2
	}, 2*time.Second, 10*time.Millisecond)

	// then — all 4 sessions should have been delivered
	all := child.allSessions()
	assert.Len(t, all, 4)
}

func TestInMemSpoolWriter_ZeroLimitsAreUnlimited(t *testing.T) {
	// given — both limits set to 0 (unlimited)
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1000),
		WithMaxAge(10*time.Minute),
		WithSweepInterval(10*time.Minute),
		WithMaxBufferedSessions(0),
		WithMaxBufferEvents(0),
	)
	require.NoError(t, err)

	// when — write many sessions
	for i := 0; i < 100; i++ {
		require.NoError(t, w.Write(session("A")))
	}

	// Allow writes to be picked up
	time.Sleep(50 * time.Millisecond)

	// then — close and verify all sessions were buffered
	cleanup()
	all := child.allSessions()
	assert.Len(t, all, 100, "with zero limits, all sessions should be buffered")
}

func TestInMemSpoolWriter_SessionLimitAcrossProperties(t *testing.T) {
	// given — limit allows 3 sessions total across properties
	child := &recordingWriter{}
	w, cleanup, err := NewInMemSpoolWriter(child,
		WithMaxSessions(1000),
		WithMaxAge(10*time.Minute),
		WithSweepInterval(10*time.Minute),
		WithMaxBufferedSessions(3),
		WithMaxBufferEvents(0),
	)
	require.NoError(t, err)

	// when — write 2 for "A" and 2 for "B" (4 total, only 3 fit)
	require.NoError(t, w.Write(session("A"), session("A"), session("B"), session("B")))

	// Allow the write to be picked up
	time.Sleep(50 * time.Millisecond)

	// then — close and verify only 3 sessions total were buffered
	cleanup()
	all := child.allSessions()
	assert.Len(t, all, 3, "session limit applies across all properties")
}
