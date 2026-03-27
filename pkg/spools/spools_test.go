package spools

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"

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
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	// when
	require.NoError(t, s.Append("prop1", []byte("hello")))
	require.NoError(t, s.Append("prop1", []byte("world")))
	require.NoError(t, s.Append("prop2", []byte("foo")))

	collected := make(map[string][][]byte)
	err = s.Flush(func(key string, _ string, frames [][]byte) error {
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

	// Active files should be gone, inflight files should be cleaned up.
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestFlushCallbackError_LeavesInflight(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("data")))

	// when — callback fails
	callbackErr := fmt.Errorf("downstream failure")
	err = s.Flush(func(_ string, _ string, _ [][]byte) error {
		return callbackErr
	})

	// then — flush returns error, inflight file remains
	assert.ErrorIs(t, err, callbackErr)

	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "prop1.spool.inflight", entries[0].Name())
}

func TestFlushRetryInflightWithoutRestart(t *testing.T) {
	// given — append data, first flush fails leaving inflight
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("frame-a")))
	require.NoError(t, s.Append("prop1", []byte("frame-b")))

	callCount := 0
	err = s.Flush(func(_ string, _ string, _ [][]byte) error {
		callCount++
		return fmt.Errorf("transient failure")
	})
	require.Error(t, err)
	assert.Equal(t, 1, callCount)

	// Inflight file should exist.
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "prop1.spool.inflight", entries[0].Name())

	// when — second flush succeeds without restart
	var retried [][]byte
	err = s.Flush(func(_ string, _ string, frames [][]byte) error {
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

func TestFlushWithInflightAndNewActive_NoRenameCollision(t *testing.T) {
	// given — first flush fails leaving inflight, then new data arrives
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("old-data")))

	err = s.Flush(func(_ string, _ string, _ [][]byte) error {
		return fmt.Errorf("transient failure")
	})
	require.Error(t, err)

	// New writes arrive after the failed flush.
	require.NoError(t, s.Append("prop1", []byte("new-data")))

	// Both files should exist: inflight from failed flush and new active.
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// when — second flush retries the inflight; active file is deferred
	var retriedFrames [][]byte
	flushCallCount := 0
	err = s.Flush(func(_ string, _ string, frames [][]byte) error {
		flushCallCount++
		retriedFrames = frames
		return nil
	})

	// then — only the old inflight was flushed (one callback call)
	require.NoError(t, err)
	assert.Equal(t, 1, flushCallCount)
	require.Len(t, retriedFrames, 1)
	assert.Equal(t, []byte("old-data"), retriedFrames[0])

	// The active file should still be present for the next flush cycle.
	entries, err = afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "prop1.spool", entries[0].Name())

	// when — third flush picks up the remaining active file
	var finalFrames [][]byte
	err = s.Flush(func(_ string, _ string, frames [][]byte) error {
		finalFrames = frames
		return nil
	})

	// then — new data flushed, directory empty
	require.NoError(t, err)
	require.Len(t, finalFrames, 1)
	assert.Equal(t, []byte("new-data"), finalFrames[0])

	entries, err = afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestFlushInflightRetryFailsAgain_ActiveStillDeferred(t *testing.T) {
	// given — inflight exists and active has new data; retry also fails
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("original")))
	err = s.Flush(func(_ string, _ string, _ [][]byte) error {
		return fmt.Errorf("fail-1")
	})
	require.Error(t, err)

	require.NoError(t, s.Append("prop1", []byte("additional")))

	// when — second flush also fails for the inflight retry
	err = s.Flush(func(_ string, _ string, _ [][]byte) error {
		return fmt.Errorf("fail-2")
	})

	// then — error returned, both files still present
	require.Error(t, err)
	entries, err := afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	names := []string{entries[0].Name(), entries[1].Name()}
	assert.Contains(t, names, "prop1.spool")
	assert.Contains(t, names, "prop1.spool.inflight")

	// when — third flush succeeds
	callKeys := make(map[string]bool)
	err = s.Flush(func(key string, _ string, _ [][]byte) error {
		callKeys[key] = true
		return nil
	})

	// then — inflight was retried; active still deferred because inflight existed
	require.NoError(t, err)
	assert.True(t, callKeys["prop1"])

	// One more flush to drain the remaining active file
	var lastFrames [][]byte
	err = s.Flush(func(_ string, _ string, frames [][]byte) error {
		lastFrames = frames
		return nil
	})
	require.NoError(t, err)
	require.Len(t, lastFrames, 1)
	assert.Equal(t, []byte("additional"), lastFrames[0])

	entries, err = afero.ReadDir(fs, "/data/spools")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestRecoverInflightOnNew(t *testing.T) {
	// given — simulate crash remnant: inflight file exists
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/data/spools", 0o755))
	writeRawFrame(t, fs, "/data/spools/prop1.spool.inflight", []byte("recovered"))

	// when — New triggers Recover
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	// then — inflight file should be gone, data should be in active file
	exists, err := afero.Exists(fs, "/data/spools/prop1.spool.inflight")
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = afero.Exists(fs, "/data/spools/prop1.spool")
	require.NoError(t, err)
	assert.True(t, exists)

	// Flush should yield the recovered frame.
	var frames [][]byte
	err = s.Flush(func(_ string, _ string, f [][]byte) error {
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
	writeRawFrame(t, fs, "/data/spools/prop1.spool.inflight", []byte("crashed"))

	// when
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	// then — flush should have both frames (existing first, then recovered)
	var frames [][]byte
	err = s.Flush(func(_ string, _ string, f [][]byte) error {
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

	// Write one valid frame manually, then append truncated data.
	writeRawFrame(t, fs, "/data/spools/prop1.spool.inflight", []byte("good"))

	// Append partial header (2 bytes instead of 4).
	f, err := fs.OpenFile("/data/spools/prop1.spool.inflight", os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerms)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x05, 0x00}) // partial header
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// when — New triggers Recover which reads inflight
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	// then — the good frame was recovered; truncated frame was dropped
	var frames [][]byte
	err = s.Flush(func(_ string, _ string, f [][]byte) error {
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

	writeRawFrame(t, fs, "/data/spools/prop1.spool.inflight", []byte("ok"))

	// Write header claiming 100 bytes but only 3 bytes of payload.
	f, err := fs.OpenFile("/data/spools/prop1.spool.inflight", os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerms)
	require.NoError(t, err)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header, 100)
	_, err = f.Write(header)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x01, 0x02, 0x03})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// when
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	// then — only the first valid frame is recovered
	var frames [][]byte
	err = s.Flush(func(_ string, _ string, f [][]byte) error {
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

	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	// when
	callbackCalled := false
	err = s.Flush(func(_ string, _ string, _ [][]byte) error {
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
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("before-flush")))

	// when — flush renames active to inflight; concurrent append creates new active
	var flushedFrames [][]byte
	err = s.Flush(func(_ string, _ string, frames [][]byte) error {
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
	err = s.Flush(func(_ string, _ string, frames [][]byte) error {
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
	err = s.Flush(func(_ string, _ string, _ [][]byte) error { return nil })
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

func TestFlushPassesInflightPath(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	s, err := New(fs, "/data/spools")
	require.NoError(t, err)

	require.NoError(t, s.Append("prop1", []byte("data")))

	// when
	var receivedPath string
	err = s.Flush(func(_ string, inflightPath string, _ [][]byte) error {
		receivedPath = inflightPath
		return nil
	})

	// then
	require.NoError(t, err)
	assert.Equal(t, "/data/spools/prop1.spool.inflight", receivedPath)
}
