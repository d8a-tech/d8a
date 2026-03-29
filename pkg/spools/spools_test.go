package spools

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeRawFrame(t *testing.T, fs afero.Fs, path string, payload []byte) {
	t.Helper()
	f, err := fs.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerms)
	require.NoError(t, err)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header, uint32(len(payload)))
	_, err = f.Write(header)
	require.NoError(t, err)
	_, err = f.Write(payload)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// recordingStrategy records calls and optionally returns an error.
type recordingStrategy struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (r *recordingStrategy) OnExceededFailures(fs afero.Fs, inflightPath string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, inflightPath)
	if r.err != nil {
		return r.err
	}
	// Default behavior: delete the file (same as deleteStrategy).
	return fs.Remove(inflightPath)
}

func (r *recordingStrategy) getCalls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

// sequentialClock returns a nowFunc that produces monotonically increasing
// timestamps starting from the given nanosecond value.
func sequentialClock(startNano int64) func() time.Time {
	v := startNano
	return func() time.Time {
		cur := atomic.AddInt64(&v, 1)
		return time.Unix(0, cur)
	}
}

func TestSanitizeKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "no special chars", key: "abc123", want: "abc123"},
		{name: "forward slash", key: "a/b/c", want: "a_b_c"},
		{name: "backslash", key: "a\\b\\c", want: "a_b_c"},
		{name: "mixed slashes", key: "a/b\\c", want: "a_b_c"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, sanitizeKey(tt.key))
		})
	}
}

func TestAppendAndFlush(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	// when
	require.NoError(t, s.Append("prop1", []byte("hello")))
	require.NoError(t, s.Append("prop1", []byte("world")))
	require.NoError(t, s.Append("prop2", []byte("foo")))

	collected := make(map[string][][]byte)
	err = s.Flush(func(key string, frames [][]byte) error {
		collected[key] = frames
		return nil
	})

	// then
	require.NoError(t, err)

	frames1, ok := collected["prop1"]
	require.True(t, ok)
	assert.Equal(t, 2, len(frames1))
	assert.Equal(t, []byte("hello"), frames1[0])
	assert.Equal(t, []byte("world"), frames1[1])

	frames2, ok := collected["prop2"]
	require.True(t, ok)
	assert.Equal(t, 1, len(frames2))
	assert.Equal(t, []byte("foo"), frames2[0])

	// Active and inflight files should all be cleaned up.
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestFlushCallbackError_LeavesInflight(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("data")))

	// when — callback fails
	callbackErr := fmt.Errorf("downstream failure")
	err = s.Flush(func(_ string, _ [][]byte) error {
		return callbackErr
	})

	// then — flush returns error, inflight file remains with timestamped name
	assert.ErrorIs(t, err, callbackErr)

	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.True(t, isInflightFile(entries[0].Name()), "expected inflight file, got %q", entries[0].Name())
	assert.Equal(t, "prop1", keyFromInflight(entries[0].Name()))
}

func TestFlushRetryInflightWithoutRestart(t *testing.T) {
	// given — append data, first flush fails leaving inflight
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("frame-a")))
	require.NoError(t, s.Append("prop1", []byte("frame-b")))

	callCount := 0
	err = s.Flush(func(_ string, _ [][]byte) error {
		callCount++
		return fmt.Errorf("transient failure")
	})
	require.Error(t, err)
	assert.Equal(t, 1, callCount)

	// Inflight file should exist.
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.True(t, isInflightFile(entries[0].Name()))

	// when — second flush succeeds without restart
	var retried [][]byte
	err = s.Flush(func(_ string, frames [][]byte) error {
		retried = frames
		return nil
	})

	// then — same frames delivered, inflight cleaned up
	require.NoError(t, err)
	require.Len(t, retried, 2)
	assert.Equal(t, []byte("frame-a"), retried[0])
	assert.Equal(t, []byte("frame-b"), retried[1])

	entries, err = afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestFlushWithInflightAndNewActive_BothFlushedInOrder(t *testing.T) {
	// given — first flush fails leaving inflight, then new data arrives
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("old-data")))

	err = s.Flush(func(_ string, _ [][]byte) error {
		return fmt.Errorf("transient failure")
	})
	require.Error(t, err)

	// New writes arrive after the failed flush.
	require.NoError(t, s.Append("prop1", []byte("new-data")))

	// Both files should exist: inflight from failed flush and new active.
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// when — second flush: active gets renamed to a new inflight, both are processed
	// in order (old first). Callback succeeds for old, then new.
	var callOrder []string
	err = s.Flush(func(_ string, frames [][]byte) error {
		callOrder = append(callOrder, string(frames[0]))
		return nil
	})

	// then — both flushed in order, directory empty
	require.NoError(t, err)
	require.Len(t, callOrder, 2)
	assert.Equal(t, "old-data", callOrder[0])
	assert.Equal(t, "new-data", callOrder[1])

	entries, err = afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestFlushWithInflightAndNewActive_OldestFailsSkipsNewer(t *testing.T) {
	// given — first flush fails, new data arrives
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("original")))
	err = s.Flush(func(_ string, _ [][]byte) error {
		return fmt.Errorf("fail-1")
	})
	require.Error(t, err)

	require.NoError(t, s.Append("prop1", []byte("additional")))

	// when — second flush: oldest fails again -> newer is skipped
	callCount := 0
	err = s.Flush(func(_ string, _ [][]byte) error {
		callCount++
		return fmt.Errorf("fail-2")
	})

	// then — callback called only once (oldest), both inflight files remain
	require.Error(t, err)
	assert.Equal(t, 1, callCount)

	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	// Two inflight files (old inflight + newly renamed active)
	require.Len(t, entries, 2)
	for _, e := range entries {
		assert.True(t, isInflightFile(e.Name()), "expected inflight, got %q", e.Name())
	}

	// when — third flush succeeds
	var flushOrder []string
	err = s.Flush(func(_ string, frames [][]byte) error {
		flushOrder = append(flushOrder, string(frames[0]))
		return nil
	})

	// then — both flushed in order, directory empty
	require.NoError(t, err)
	require.Len(t, flushOrder, 2)
	assert.Equal(t, "original", flushOrder[0])
	assert.Equal(t, "additional", flushOrder[1])

	entries, err = afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestMultipleInflightsPerKey_ActiveAlwaysRenamed(t *testing.T) {
	// given — existing inflight + active file
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	// First append + failed flush creates inflight-1
	require.NoError(t, s.Append("prop1", []byte("batch-1")))
	_ = s.Flush(func(_ string, _ [][]byte) error {
		return fmt.Errorf("fail")
	})

	// Second append creates a new active file
	require.NoError(t, s.Append("prop1", []byte("batch-2")))

	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 2) // 1 inflight + 1 active

	// when — flush: active should always be renamed, even with pending inflight
	_ = s.Flush(func(_ string, _ [][]byte) error {
		return fmt.Errorf("fail again")
	})

	// then — no active file should remain; both should be inflight
	entries, err = afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	for _, e := range entries {
		assert.True(t, isInflightFile(e.Name()), "expected inflight, got %q", e.Name())
		assert.False(t, isActiveFile(e.Name()), "active file should not remain after flush")
	}
}

func TestMultipleInflightsPerKey_OrderingPreserved(t *testing.T) {
	// given — create 3 inflight files by appending + failing flushes
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		require.NoError(t, s.Append("prop1", []byte(fmt.Sprintf("batch-%d", i))))
		_ = s.Flush(func(_ string, _ [][]byte) error {
			return fmt.Errorf("fail")
		})
	}

	// Verify 3 inflight files exist
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 3)

	// when — flush with success: all 3 should be flushed in order
	var order []string
	err = s.Flush(func(_ string, frames [][]byte) error {
		order = append(order, string(frames[0]))
		return nil
	})

	// then — delivered oldest first
	require.NoError(t, err)
	require.Len(t, order, 3)
	assert.Equal(t, "batch-0", order[0])
	assert.Equal(t, "batch-1", order[1])
	assert.Equal(t, "batch-2", order[2])

	entries, err = afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestRecoverInflightOnNew(t *testing.T) {
	// given — simulate crash remnant: inflight file with new naming
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/data/spools", 0o755))
	writeRawFrame(t, fs, "/data/spools/prop1.spool.inflight.1000", []byte("recovered"))

	// when — New triggers Recover
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(2000)))
	require.NoError(t, err)

	// then — inflight file should be gone, data should be in active file
	exists, err := afero.Exists(fs, "/data/spools/prop1.spool.inflight.1000")
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = afero.Exists(fs, "/data/spools/prop1.spool")
	require.NoError(t, err)
	assert.True(t, exists)

	// Flush should yield the recovered frame.
	var frames [][]byte
	err = s.Flush(func(_ string, f [][]byte) error {
		frames = f
		return nil
	})
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, []byte("recovered"), frames[0])
}

func TestRecoverMergesIntoExistingActive(t *testing.T) {
	// given — both active and inflight exist for same key
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/data/spools", 0o755))
	writeRawFrame(t, fs, "/data/spools/prop1.spool", []byte("existing"))
	writeRawFrame(t, fs, "/data/spools/prop1.spool.inflight.1000", []byte("crashed"))

	// when
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(2000)))
	require.NoError(t, err)

	// then — flush should have both frames (existing first, then recovered)
	var frames [][]byte
	err = s.Flush(func(_ string, f [][]byte) error {
		frames = f
		return nil
	})
	require.NoError(t, err)
	require.Len(t, frames, 2)
	assert.Equal(t, []byte("existing"), frames[0])
	assert.Equal(t, []byte("crashed"), frames[1])
}

func TestTruncatedTrailingFrame(t *testing.T) {
	// given — inflight file with one valid frame and truncated second
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/data/spools", 0o755))

	inflightPath := "/data/spools/prop1.spool.inflight.1000"
	writeRawFrame(t, fs, inflightPath, []byte("good"))

	// Append partial header (2 bytes instead of 4).
	f, err := fs.OpenFile(inflightPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerms)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x05, 0x00}) // partial header
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// when — New triggers Recover which reads inflight
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(2000)))
	require.NoError(t, err)

	// then — the good frame was recovered; truncated frame was dropped
	var frames [][]byte
	err = s.Flush(func(_ string, f [][]byte) error {
		frames = f
		return nil
	})
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, []byte("good"), frames[0])
}

func TestTruncatedPayload(t *testing.T) {
	// given — file with valid header but short payload
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/data/spools", 0o755))

	inflightPath := "/data/spools/prop1.spool.inflight.1000"
	writeRawFrame(t, fs, inflightPath, []byte("ok"))

	// Write header claiming 100 bytes but only 3 bytes of payload.
	f, err := fs.OpenFile(inflightPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerms)
	require.NoError(t, err)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header, 100)
	_, err = f.Write(header)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x01, 0x02, 0x03})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// when
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(2000)))
	require.NoError(t, err)

	// then — only the first valid frame is recovered
	var frames [][]byte
	err = s.Flush(func(_ string, f [][]byte) error {
		frames = f
		return nil
	})
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, []byte("ok"), frames[0])
}

func TestEmptyActiveFileFlush(t *testing.T) {
	// given — empty spool file
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/data/spools", 0o755))
	f, err := fs.Create("/data/spools/prop1.spool")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	// when
	callbackCalled := false
	err = s.Flush(func(_ string, _ [][]byte) error {
		callbackCalled = true
		return nil
	})

	// then — callback not called for empty file
	require.NoError(t, err)
	assert.False(t, callbackCalled)

	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestConcurrentAppendDuringFlush(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("before-flush")))

	// when — flush renames active to inflight; concurrent append creates new active
	var flushedFrames [][]byte
	err = s.Flush(func(_ string, frames [][]byte) error {
		// Append happens while flush callback runs (new active file).
		appendErr := s.Append("prop1", []byte("during-flush"))
		require.NoError(t, appendErr)
		flushedFrames = frames
		return nil
	})
	require.NoError(t, err)

	// then — flushed frames contain only pre-flush data
	require.Len(t, flushedFrames, 1)
	assert.Equal(t, []byte("before-flush"), flushedFrames[0])

	// Second flush picks up the concurrent append.
	var secondFrames [][]byte
	err = s.Flush(func(_ string, frames [][]byte) error {
		secondFrames = frames
		return nil
	})
	require.NoError(t, err)
	require.Len(t, secondFrames, 1)
	assert.Equal(t, []byte("during-flush"), secondFrames[0])
}

func TestCloseRejectsAppend(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	// when
	require.NoError(t, s.Close())

	// then
	err = s.Append("prop1", []byte("data"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestCloseRejectsFlush(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	// when
	require.NoError(t, s.Close())

	// then
	err = s.Flush(func(_ string, _ [][]byte) error { return nil })
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestKeySanitizationInFilenames(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	// when — keys with slashes
	require.NoError(t, s.Append("org/prop/123", []byte("data1")))
	require.NoError(t, s.Append("org\\prop\\456", []byte("data2")))

	// then — files exist with sanitized names
	exists, err := afero.Exists(fs, "/data/spools/org_prop_123.spool")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = afero.Exists(fs, "/data/spools/org_prop_456.spool")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestFailureStrategy_DeleteOnExceededFailures(t *testing.T) {
	// given — spool with maxFailures=3 and recording strategy
	fs := afero.NewMemMapFs()
	strategy := &recordingStrategy{}
	s, err := New(fs, "/data/spools",
		WithFailureStrategy(strategy),
		WithMaxFailures(3),
		WithNowFunc(sequentialClock(1000)),
	)
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("doomed")))

	// when — fail 3 times to hit the threshold
	for i := 0; i < 3; i++ {
		_ = s.Flush(func(_ string, _ [][]byte) error {
			return fmt.Errorf("fail-%d", i)
		})
	}

	// then — strategy was called once (for the single inflight file)
	calls := strategy.getCalls()
	require.Len(t, calls, 1)
	assert.True(t, isInflightFile(calls[0][len("/data/spools/"):]))
	assert.Equal(t, "prop1", keyFromInflight(calls[0][len("/data/spools/"):]))

	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries, "inflight file should be removed by strategy")
}

func TestFailureStrategy_SuccessResetsCounter(t *testing.T) {
	// given — spool with maxFailures=3
	fs := afero.NewMemMapFs()
	strategy := &recordingStrategy{}
	s, err := New(fs, "/data/spools",
		WithFailureStrategy(strategy),
		WithMaxFailures(3),
		WithNowFunc(sequentialClock(1000)),
	)
	require.NoError(t, err)

	// Fail twice
	require.NoError(t, s.Append("prop1", []byte("data-round1")))
	for i := 0; i < 2; i++ {
		_ = s.Flush(func(_ string, _ [][]byte) error {
			return fmt.Errorf("transient")
		})
	}

	// when — success on third flush resets counter
	err = s.Flush(func(_ string, _ [][]byte) error {
		return nil
	})
	require.NoError(t, err)

	// Write new data and fail 2 more times — should NOT trigger strategy
	// because counter was reset.
	require.NoError(t, s.Append("prop1", []byte("data-round2")))
	for i := 0; i < 2; i++ {
		_ = s.Flush(func(_ string, _ [][]byte) error {
			return fmt.Errorf("transient")
		})
	}

	// then — strategy was never called (counter reset at 2, never reached 3)
	calls := strategy.getCalls()
	assert.Empty(t, calls)
}

func TestFailureStrategy_DefaultIsDelete(t *testing.T) {
	// given — spool with maxFailures=1 and no explicit strategy (default=delete)
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithMaxFailures(1),
		WithNowFunc(sequentialClock(1000)),
	)
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("will-be-deleted")))

	// when — single failure triggers default delete strategy
	_ = s.Flush(func(_ string, _ [][]byte) error {
		return fmt.Errorf("permanent failure")
	})

	// then — inflight file was deleted by default strategy
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestFailureStrategy_QuarantineRenames(t *testing.T) {
	// given — spool with maxFailures=1 and quarantine strategy
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithFailureStrategy(NewQuarantineStrategy()),
		WithMaxFailures(1),
		WithNowFunc(sequentialClock(1000)),
	)
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("quarantine-me")))

	// when — single failure triggers quarantine
	_ = s.Flush(func(_ string, _ [][]byte) error {
		return fmt.Errorf("permanent failure")
	})

	// then — inflight file renamed to .quarantine
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Contains(t, entries[0].Name(), ".quarantine")
	assert.Contains(t, entries[0].Name(), "prop1")
}

func TestFailureStrategy_MultipleKeysTrackedIndependently(t *testing.T) {
	// given — spool with maxFailures=2
	fs := afero.NewMemMapFs()
	strategy := &recordingStrategy{}
	s, err := New(fs, "/data/spools",
		WithFailureStrategy(strategy),
		WithMaxFailures(2),
		WithNowFunc(sequentialClock(1000)),
	)
	require.NoError(t, err)

	require.NoError(t, s.Append("key-a", []byte("a-data")))
	require.NoError(t, s.Append("key-b", []byte("b-data")))

	// when — fail once for both keys
	_ = s.Flush(func(_ string, _ [][]byte) error {
		return fmt.Errorf("fail-1")
	})

	// Succeed for key-a on second flush, fail for key-b
	_ = s.Flush(func(key string, _ [][]byte) error {
		if key == "key-b" {
			return fmt.Errorf("fail-2")
		}
		return nil
	})

	// then — strategy called only for key-b (reached threshold of 2)
	calls := strategy.getCalls()
	require.Len(t, calls, 1)
	assert.Contains(t, calls[0], "key-b")
}

func TestFailureStrategy_CleansAllInflightsForKey(t *testing.T) {
	// given — spool with maxFailures=3, create multiple inflight files
	fs := afero.NewMemMapFs()
	strategy := &recordingStrategy{}
	s, err := New(fs, "/data/spools",
		WithFailureStrategy(strategy),
		WithMaxFailures(3),
		WithNowFunc(sequentialClock(1000)),
	)
	require.NoError(t, err)

	// Create 3 inflight files by appending + failing flushes.
	// Each flush: active renamed to inflight, callback fails, inflight stays.
	// Next append creates new active. Repeat.
	for i := 0; i < 3; i++ {
		require.NoError(t, s.Append("prop1", []byte(fmt.Sprintf("batch-%d", i))))
		_ = s.Flush(func(_ string, _ [][]byte) error {
			return fmt.Errorf("fail-%d", i)
		})
	}

	// then — the 3rd flush hit the threshold; strategy was called for all
	// inflight files for this key. Because each flush cycle renames the active
	// file into a new inflight, by the 3rd cycle there are 3 inflight files.
	// handleFlushFailure passes paths[i:] (starting from the oldest, which
	// always fails first), so all 3 are passed to the strategy.
	calls := strategy.getCalls()
	require.Len(t, calls, 3, "strategy should have been called for 3 inflight files")
	for _, c := range calls {
		assert.Contains(t, c, "prop1")
		assert.True(t, isInflightFile(c[len("/data/spools/"):]))
	}

	// All inflight files should be deleted by the strategy.
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries, "all inflight files should be removed by strategy")
}

func TestRecoverMultipleInflights(t *testing.T) {
	// given — multiple inflight files for the same key (crash recovery)
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/data/spools", 0o755))
	writeRawFrame(t, fs, "/data/spools/prop1.spool.inflight.1000", []byte("old"))
	writeRawFrame(t, fs, "/data/spools/prop1.spool.inflight.2000", []byte("new"))

	// when — New triggers Recover
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(3000)))
	require.NoError(t, err)

	// then — both frames recovered into active, inflight files gone
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "prop1.spool", entries[0].Name())

	// Flush should yield both frames
	var frames [][]byte
	err = s.Flush(func(_ string, f [][]byte) error {
		frames = f
		return nil
	})
	require.NoError(t, err)
	require.Len(t, frames, 2)
	assert.Equal(t, []byte("old"), frames[0])
	assert.Equal(t, []byte("new"), frames[1])
}

func TestIsInflightFile(t *testing.T) {
	tests := []struct {
		name string
		file string
		want bool
	}{
		{name: "active file", file: "prop1.spool", want: false},
		{name: "inflight file", file: "prop1.spool.inflight.1001", want: true},
		{name: "quarantined file", file: "prop1.spool.inflight.1001.quarantine", want: true},
		{name: "unrelated file", file: "something.txt", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isInflightFile(tt.file))
		})
	}
}

func TestIsActiveFile(t *testing.T) {
	tests := []struct {
		name string
		file string
		want bool
	}{
		{name: "active file", file: "prop1.spool", want: true},
		{name: "inflight file", file: "prop1.spool.inflight.1001", want: false},
		{name: "unrelated file", file: "something.txt", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isActiveFile(tt.file))
		})
	}
}

func TestKeyFromInflight(t *testing.T) {
	tests := []struct {
		name string
		file string
		want string
	}{
		{name: "normal inflight", file: "prop1.spool.inflight.1001", want: "prop1"},
		{name: "sanitized key", file: "org_prop_123.spool.inflight.5000", want: "org_prop_123"},
		{name: "not inflight", file: "prop1.spool", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, keyFromInflight(tt.file))
		})
	}
}

func TestInflightFilesAreSortedByTimestamp(t *testing.T) {
	// given — inflight files with varying timestamps
	names := []string{
		"/data/spools/prop1.spool.inflight.3000",
		"/data/spools/prop1.spool.inflight.1000",
		"/data/spools/prop1.spool.inflight.2000",
	}

	// when — sorted lexicographically
	sort.Strings(names)

	// then — oldest first
	assert.Equal(t, "/data/spools/prop1.spool.inflight.1000", names[0])
	assert.Equal(t, "/data/spools/prop1.spool.inflight.2000", names[1])
	assert.Equal(t, "/data/spools/prop1.spool.inflight.3000", names[2])
}
