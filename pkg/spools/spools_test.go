package spools

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

// drainNext consumes all batches from next and returns the concatenated frames.
func drainNext(next func() ([][]byte, error)) ([][]byte, error) {
	var all [][]byte
	for {
		batch, err := next()
		if errors.Is(err, io.EOF) {
			return all, nil
		}
		if err != nil {
			return nil, err
		}
		all = append(all, batch...)
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
	err = s.Flush(func(key string, next func() ([][]byte, error)) error {
		frames, drainErr := drainNext(next)
		if drainErr != nil {
			return drainErr
		}
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
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		retried, drainErr = drainNext(next)
		return drainErr
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

	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		frames, drainErr := drainNext(next)
		if drainErr != nil {
			return drainErr
		}
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
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
		return fmt.Errorf("fail-1")
	})
	require.Error(t, err)

	require.NoError(t, s.Append("prop1", []byte("additional")))

	// when — second flush: oldest fails again -> newer is skipped
	callCount := 0
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		frames, drainErr := drainNext(next)
		if drainErr != nil {
			return drainErr
		}
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
	_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
		return fmt.Errorf("fail")
	})

	// Second append creates a new active file
	require.NoError(t, s.Append("prop1", []byte("batch-2")))

	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 2) // 1 inflight + 1 active

	// when — flush: active should always be renamed, even with pending inflight
	_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
		_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
			return fmt.Errorf("fail")
		})
	}

	// Verify 3 inflight files exist
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 3)

	// when — flush with success: all 3 should be flushed in order
	var order []string
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		frames, drainErr := drainNext(next)
		if drainErr != nil {
			return drainErr
		}
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
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		frames, drainErr = drainNext(next)
		return drainErr
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
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		frames, drainErr = drainNext(next)
		return drainErr
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
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		frames, drainErr = drainNext(next)
		return drainErr
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
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		frames, drainErr = drainNext(next)
		return drainErr
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
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		// Append happens while flush callback runs (new active file).
		appendErr := s.Append("prop1", []byte("during-flush"))
		require.NoError(t, appendErr)
		var drainErr error
		flushedFrames, drainErr = drainNext(next)
		return drainErr
	})
	require.NoError(t, err)

	// then — flushed frames contain only pre-flush data
	require.Len(t, flushedFrames, 1)
	assert.Equal(t, []byte("before-flush"), flushedFrames[0])

	// Second flush picks up the concurrent append.
	var secondFrames [][]byte
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		secondFrames, drainErr = drainNext(next)
		return drainErr
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
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error { return nil })
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
		_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
		_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
			return fmt.Errorf("transient")
		})
	}

	// when — success on third flush resets counter
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
		return nil
	})
	require.NoError(t, err)

	// Write new data and fail 2 more times — should NOT trigger strategy
	// because counter was reset.
	require.NoError(t, s.Append("prop1", []byte("data-round2")))
	for i := 0; i < 2; i++ {
		_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
	_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
	_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
	_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
		return fmt.Errorf("fail-1")
	})

	// Succeed for key-a on second flush, fail for key-b
	_ = s.Flush(func(key string, _ func() ([][]byte, error)) error {
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
		_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
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
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		frames, drainErr = drainNext(next)
		return drainErr
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
		{name: "quarantined file", file: "prop1.spool.inflight.1001.quarantine", want: false},
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

func TestConcurrentFlushAndAppendNoRaceOnFailures(t *testing.T) {
	// This test exercises concurrent Flush (with callback failures) and
	// Append to verify there is no data race on failuresByKey. Run with
	// -race to detect unsynchronized map access.

	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithNowFunc(sequentialClock(5000)),
		WithMaxFailures(5),
	)
	require.NoError(t, err)

	const goroutines = 4
	const iterations = 20

	// when — concurrent appends and flushes, with flushes sometimes failing
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		key := fmt.Sprintf("key%d", g)

		go func(k string) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = s.Append(k, []byte(fmt.Sprintf("payload-%d", i)))
			}
		}(key)
	}

	var flushCount atomic.Int32
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
					// Fail roughly half the time to exercise handleFlushFailure.
					if flushCount.Add(1)%2 == 0 {
						return fmt.Errorf("transient error")
					}
					return nil
				})
			}
		}()
	}

	wg.Wait()

	// then — no race detected (the race detector flags violations automatically).
	// Final flush to drain remaining data.
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error { return nil })
	assert.NoError(t, err)
}

// --- New tests for batched flush iteration ---

func TestFlushBatchSize_MultipleBatches(t *testing.T) {
	// given — spool with batch size of 2, 5 frames appended
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithNowFunc(sequentialClock(1000)),
		WithFlushBatchSize(2),
	)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, s.Append("prop1", []byte(fmt.Sprintf("f%d", i))))
	}

	// when — flush with batching
	var batches [][]string
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		for {
			batch, nextErr := next()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				return nextErr
			}
			var strs []string
			for _, f := range batch {
				strs = append(strs, string(f))
			}
			batches = append(batches, strs)
		}
		return nil
	})

	// then — 3 batches: [f0,f1], [f2,f3], [f4]
	require.NoError(t, err)
	require.Len(t, batches, 3)
	assert.Equal(t, []string{"f0", "f1"}, batches[0])
	assert.Equal(t, []string{"f2", "f3"}, batches[1])
	assert.Equal(t, []string{"f4"}, batches[2])
}

func TestFlushBatchSize_ExactMultiple(t *testing.T) {
	// given — 4 frames, batch size 2 — exact division
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithNowFunc(sequentialClock(1000)),
		WithFlushBatchSize(2),
	)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, s.Append("prop1", []byte(fmt.Sprintf("f%d", i))))
	}

	// when
	var batchCount int
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		for {
			batch, nextErr := next()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				return nextErr
			}
			assert.Len(t, batch, 2)
			batchCount++
		}
		return nil
	})

	// then
	require.NoError(t, err)
	assert.Equal(t, 2, batchCount)
}

func TestFlushBatchSize_ZeroMeansAllInOneBatch(t *testing.T) {
	// given — no batch size set (default 0), 5 frames
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)))
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, s.Append("prop1", []byte(fmt.Sprintf("f%d", i))))
	}

	// when
	var batchCount int
	var allFrames [][]byte
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		for {
			batch, nextErr := next()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				return nextErr
			}
			batchCount++
			allFrames = append(allFrames, batch...)
		}
		return nil
	})

	// then — single batch with all 5 frames
	require.NoError(t, err)
	assert.Equal(t, 1, batchCount)
	assert.Len(t, allFrames, 5)
}

func TestFlushBatchSize_NextReturnsEOFAfterExhaustion(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithNowFunc(sequentialClock(1000)),
		WithFlushBatchSize(10),
	)
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("only-one")))

	// when — drain and then call next again
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		batch, nextErr := next()
		require.NoError(t, nextErr)
		assert.Len(t, batch, 1)

		// Subsequent calls return EOF.
		_, nextErr = next()
		assert.ErrorIs(t, nextErr, io.EOF)
		_, nextErr = next()
		assert.ErrorIs(t, nextErr, io.EOF)
		return nil
	})
	require.NoError(t, err)
}

func TestFlushBatchSize_PartialConsumeRetryGetsFullFile(t *testing.T) {
	// given — 4 frames, batch size 2. Callback consumes one batch then fails.
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithNowFunc(sequentialClock(1000)),
		WithFlushBatchSize(2),
	)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, s.Append("prop1", []byte(fmt.Sprintf("f%d", i))))
	}

	// when — first flush: consume 1 batch then fail
	firstCall := true
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		batch, nextErr := next()
		require.NoError(t, nextErr)
		assert.Len(t, batch, 2) // consumed first batch
		if firstCall {
			firstCall = false
			return fmt.Errorf("partial failure")
		}
		return nil
	})
	require.Error(t, err)

	// then — retry gets the full file again (whole-file retry)
	var allFrames [][]byte
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		allFrames, drainErr = drainNext(next)
		return drainErr
	})
	require.NoError(t, err)
	require.Len(t, allFrames, 4)
	assert.Equal(t, "f0", string(allFrames[0]))
	assert.Equal(t, "f1", string(allFrames[1]))
	assert.Equal(t, "f2", string(allFrames[2]))
	assert.Equal(t, "f3", string(allFrames[3]))
}

func TestMaxActiveSize_RotatesOnThreshold(t *testing.T) {
	// given — maxActiveSize such that it triggers rotation after a few appends.
	// Each frame is headerSize(4) + payload bytes.
	fs := afero.NewMemMapFs()
	// Set max active size to 20 bytes. Each 5-byte payload = 9 bytes on disk.
	// After 3 appends (27 bytes) the file exceeds 20, so it should rotate.
	// But rotation happens after each append that crosses the threshold.
	s, err := New(fs, "/data/spools",
		WithNowFunc(sequentialClock(1000)),
		WithMaxActiveSize(20),
	)
	require.NoError(t, err)

	// when — append 5 frames of 5 bytes each
	for i := 0; i < 5; i++ {
		require.NoError(t, s.Append("prop1", []byte(fmt.Sprintf("p%03d", i))))
	}

	// then — should have rotated active file(s), producing inflight files
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)

	inflightCount := 0
	activeCount := 0
	for _, e := range entries {
		if isInflightFile(e.Name()) {
			inflightCount++
		}
		if isActiveFile(e.Name()) {
			activeCount++
		}
	}
	// At least one rotation should have happened.
	assert.GreaterOrEqual(t, inflightCount, 1, "expected at least one rotated inflight file")
	// There should be at most one active file (current tail).
	assert.LessOrEqual(t, activeCount, 1, "at most one active file")

	// Flush should retrieve all 5 frames in order.
	var allFrames [][]byte
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		frames, drainErr := drainNext(next)
		if drainErr != nil {
			return drainErr
		}
		allFrames = append(allFrames, frames...)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, allFrames, 5)
	for i := 0; i < 5; i++ {
		assert.Equal(t, fmt.Sprintf("p%03d", i), string(allFrames[i]))
	}
}

func TestMaxActiveSize_SmallEnoughForSingleFrame(t *testing.T) {
	// given — maxActiveSize = 1 byte, so every single frame triggers rotation.
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithNowFunc(sequentialClock(1000)),
		WithMaxActiveSize(1),
	)
	require.NoError(t, err)

	// when — append 3 frames
	for i := 0; i < 3; i++ {
		require.NoError(t, s.Append("prop1", []byte(fmt.Sprintf("x%d", i))))
	}

	// then — 3 inflight files, no active file (each was immediately rotated)
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)

	inflightCount := 0
	for _, e := range entries {
		if isInflightFile(e.Name()) {
			inflightCount++
		}
		assert.False(t, isActiveFile(e.Name()), "no active file expected, got %q", e.Name())
	}
	assert.Equal(t, 3, inflightCount)

	// Flush retrieves all 3 frames in order.
	var allFrames [][]byte
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		frames, drainErr := drainNext(next)
		if drainErr != nil {
			return drainErr
		}
		allFrames = append(allFrames, frames...)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, allFrames, 3)
	assert.Equal(t, "x0", string(allFrames[0]))
	assert.Equal(t, "x1", string(allFrames[1]))
	assert.Equal(t, "x2", string(allFrames[2]))
}

func TestMaxActiveSize_ZeroMeansNoLimit(t *testing.T) {
	// given — default (0), no rotation
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithNowFunc(sequentialClock(1000)),
	)
	require.NoError(t, err)

	// when — append many frames
	for i := 0; i < 20; i++ {
		require.NoError(t, s.Append("prop1", []byte(fmt.Sprintf("payload-%02d", i))))
	}

	// then — only one active file, no inflight
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.True(t, isActiveFile(entries[0].Name()))
}

func TestFlushBatchSize_MultipleInflightFiles(t *testing.T) {
	// given — two inflight files, batch size 1. Each inflight file gets
	// its own callback invocation with its own next function.
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools",
		WithNowFunc(sequentialClock(1000)),
		WithFlushBatchSize(1),
	)
	require.NoError(t, err)

	// Create two inflight files.
	require.NoError(t, s.Append("prop1", []byte("file1-a")))
	require.NoError(t, s.Append("prop1", []byte("file1-b")))
	_ = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
		return fmt.Errorf("fail")
	})

	require.NoError(t, s.Append("prop1", []byte("file2-a")))

	// when — flush succeeds, track batches per callback call
	type callBatches struct {
		batches [][]string
	}
	var calls []callBatches

	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var cb callBatches
		for {
			batch, nextErr := next()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				return nextErr
			}
			var strs []string
			for _, f := range batch {
				strs = append(strs, string(f))
			}
			cb.batches = append(cb.batches, strs)
		}
		calls = append(calls, cb)
		return nil
	})

	// then — two callback invocations (one per inflight file)
	require.NoError(t, err)
	require.Len(t, calls, 2)

	// First call: file1 with 2 frames, batch size 1 → 2 batches
	require.Len(t, calls[0].batches, 2)
	assert.Equal(t, []string{"file1-a"}, calls[0].batches[0])
	assert.Equal(t, []string{"file1-b"}, calls[0].batches[1])

	// Second call: file2 with 1 frame, batch size 1 → 1 batch
	require.Len(t, calls[1].batches, 1)
	assert.Equal(t, []string{"file2-a"}, calls[1].batches[0])
}

func TestFlushBatchSize_TruncatedTrailingFrameDuringFlush(t *testing.T) {
	// given — spool with one valid frame; we manually append garbage to
	// the inflight file before flushing. The incremental reader must
	// tolerate the truncation and still deliver the valid frame.
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)),
		WithFlushBatchSize(1))
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("valid")))

	// Rotate active → inflight manually via a failing flush.
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
		return fmt.Errorf("force-fail")
	})
	require.Error(t, err)

	// Find the inflight file and append a truncated header.
	entries, dirErr := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, dirErr)
	var inflightPath string
	for _, e := range entries {
		if isInflightFile(e.Name()) {
			inflightPath = "/data/spools/" + e.Name()
		}
	}
	require.NotEmpty(t, inflightPath)

	f, err := fs.OpenFile(inflightPath, os.O_WRONLY|os.O_APPEND, filePerms)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF, 0x00}) // partial header (2 of 4 bytes)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// when — flush reads incrementally
	var frames [][]byte
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		frames, drainErr = drainNext(next)
		return drainErr
	})

	// then — one valid frame recovered, truncated frame dropped
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, []byte("valid"), frames[0])
}

func TestFlushBatchSize_TruncatedPayloadDuringFlush(t *testing.T) {
	// given — spool with one valid frame and a second frame with a valid
	// header claiming 100 bytes but only 3 bytes of payload.
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools", WithNowFunc(sequentialClock(1000)),
		WithFlushBatchSize(1))
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("ok")))

	// Rotate active → inflight via failing flush.
	err = s.Flush(func(_ string, _ func() ([][]byte, error)) error {
		return fmt.Errorf("force-fail")
	})
	require.Error(t, err)

	// Append corrupt frame to the inflight file.
	entries, dirErr := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, dirErr)
	var inflightPath string
	for _, e := range entries {
		if isInflightFile(e.Name()) {
			inflightPath = "/data/spools/" + e.Name()
		}
	}
	require.NotEmpty(t, inflightPath)

	f, err := fs.OpenFile(inflightPath, os.O_WRONLY|os.O_APPEND, filePerms)
	require.NoError(t, err)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header, 100) // claims 100 bytes
	_, err = f.Write(header)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x01, 0x02, 0x03}) // only 3 bytes
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// when — flush reads incrementally
	var frames [][]byte
	err = s.Flush(func(_ string, next func() ([][]byte, error)) error {
		var drainErr error
		frames, drainErr = drainNext(next)
		return drainErr
	})

	// then — only the valid frame is delivered
	require.NoError(t, err)
	require.Len(t, frames, 1)
	assert.Equal(t, []byte("ok"), frames[0])
}
