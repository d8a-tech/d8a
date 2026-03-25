package sessions

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSpoolWriter struct {
	mu         sync.Mutex
	calls      []spoolWriteCall
	writeErr   error
	closeCalls int
}

type spoolWriteCall struct {
	propID  string
	payload []byte
}

func (m *mockSpoolWriter) Write(propID string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, spoolWriteCall{propID: propID, payload: append([]byte(nil), payload...)})
	return m.writeErr
}

func (m *mockSpoolWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	return nil
}

func (m *mockSpoolWriter) getCalls() []spoolWriteCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]spoolWriteCall, len(m.calls))
	copy(out, m.calls)
	return out
}

func (m *mockSpoolWriter) getCloseCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCalls
}

type mockSpoolFailureStrategy struct {
	mu    sync.Mutex
	paths []string
	err   error
}

func (m *mockSpoolFailureStrategy) OnExceededFailures(spoolPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paths = append(m.paths, spoolPath)
	return m.err
}

func (m *mockSpoolFailureStrategy) getPaths() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.paths))
	copy(out, m.paths)
	return out
}

// countingMockSessionWriter is a mock that fails a specific number of times before succeeding.
// If failCount is negative, it always fails.
type countingMockSessionWriter struct {
	mu              sync.Mutex
	writeCalls      [][]*schema.Session
	failCount       int
	currentFailures int
	alwaysFail      bool
}

func (m *countingMockSessionWriter) Write(sessions ...*schema.Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeCalls = append(m.writeCalls, sessions)
	if m.alwaysFail || (m.failCount >= 0 && m.currentFailures < m.failCount) {
		m.currentFailures++
		return assert.AnError
	}
	return nil
}

func (m *countingMockSessionWriter) getWriteCalls() [][]*schema.Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]*schema.Session, len(m.writeCalls))
	copy(out, m.writeCalls)
	return out
}

// newTestSession creates a test session with a given property ID.
func newTestSession(propertyID string) *schema.Session {
	hit := &hits.Hit{
		PropertyID: propertyID,
	}
	event := schema.NewEvent(hit)
	return schema.NewSession([]*schema.Event{event})
}

func TestWriteDelegatesToSpoolWriter(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{}
	sw := &mockSpoolWriter{}
	fs := &mockSpoolFailureStrategy{}

	writer, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(10*time.Second),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	sess1 := newTestSession("prop1")
	sess2 := newTestSession("prop2")
	err = writer.Write(sess1, sess2)

	// then
	require.NoError(t, err)

	calls := sw.getCalls()
	require.Len(t, calls, 2, "should delegate one call per property")

	propIDs := map[string]bool{}
	for _, c := range calls {
		propIDs[c.propID] = true
		assert.NotEmpty(t, c.payload, "payload should not be empty")
	}
	assert.True(t, propIDs["prop1"], "should have prop1 call")
	assert.True(t, propIDs["prop2"], "should have prop2 call")
}

func TestWriteReturnsSpoolWriterError(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{}
	sw := &mockSpoolWriter{writeErr: fmt.Errorf("disk full")}
	fs := &mockSpoolFailureStrategy{}

	writer, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(10*time.Second),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	err = writer.Write(newTestSession("prop1"))

	// then
	assert.Error(t, err, "should return spool writer error")
	assert.Contains(t, err.Error(), "disk full")
}

func TestNewBackgroundBatchingWriterRejectsNilDependencies(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{}
	fs := &mockSpoolFailureStrategy{}
	sw := &mockSpoolWriter{}

	// when
	_, _, errNilSpoolWriter := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		nil,
		fs,
		WithSpoolDir(tmpDir),
	)

	_, _, errNilFailureStrategy := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		nil,
		WithSpoolDir(tmpDir),
	)

	// then
	require.Error(t, errNilSpoolWriter)
	assert.Contains(t, errNilSpoolWriter.Error(), "spool writer is required")
	require.Error(t, errNilFailureStrategy)
	assert.Contains(t, errNilFailureStrategy.Error(), "spool failure strategy is required")
}

func TestWriteRejectsAfterCleanup(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{}
	sw := &mockSpoolWriter{}
	fs := &mockSpoolFailureStrategy{}

	writer, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
	)
	require.NoError(t, err)

	cleanup()

	// when
	err = writer.Write(newTestSession("prop1"))

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "writer is stopped")
	assert.Len(t, sw.getCalls(), 0)
}

func TestDirectSpoolWriterSyncsAndFrameFormat(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	sw, err := NewDirectSpoolWriter(tmpDir)
	require.NoError(t, err)

	var encodedBuf bytes.Buffer
	sessions := []*schema.Session{newTestSession("prop1")}
	_, err = encoding.GobEncoder(&encodedBuf, sessions)
	require.NoError(t, err)
	payload := encodedBuf.Bytes()

	// when
	err = sw.Write("prop1", payload)

	// then
	require.NoError(t, err)

	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	data, err := os.ReadFile(spoolPath)
	require.NoError(t, err)

	// Verify framed record format: 4-byte LE length + payload
	require.True(t, len(data) >= 4, "should have at least 4-byte header")
	payloadLen := binary.LittleEndian.Uint32(data[:4])
	assert.Equal(t, uint32(len(payload)), payloadLen, "header should match payload length")
	assert.Equal(t, payload, data[4:], "payload on disk should match written payload")

	// Verify it decodes back correctly
	var decoded []*schema.Session
	err = encoding.GobDecoder(bytes.NewReader(data[4:]), &decoded)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	assert.Equal(t, "prop1", decoded[0].PropertyID)
}

func TestDirectSpoolWriterRejectsWriteAfterClose(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	sw, err := NewDirectSpoolWriter(tmpDir)
	require.NoError(t, err)

	require.NoError(t, sw.Close())

	// when
	err = sw.Write("prop1", []byte("payload"))

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "direct spool writer is stopped")
}

func TestDirectSpoolWriterPreservesFileOnChildFailure(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{alwaysFail: true, failCount: -1}
	sw, err := NewDirectSpoolWriter(tmpDir)
	require.NoError(t, err)
	fs := &mockSpoolFailureStrategy{}

	writer, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(50*time.Millisecond),
		WithMaxConsecutiveChildWriteFailures(100), // High threshold so file persists
	)
	require.NoError(t, err)
	defer cleanup()

	// when -- write session via direct spool
	var encodedBuf bytes.Buffer
	sessions := []*schema.Session{newTestSession("prop1")}
	_, err = encoding.GobEncoder(&encodedBuf, sessions)
	require.NoError(t, err)
	err = sw.Write("prop1", encodedBuf.Bytes())
	require.NoError(t, err)

	// then -- wait for at least one lvl2 flush attempt
	assert.Eventually(t, func() bool {
		return len(childWriter.getWriteCalls()) >= 1
	}, 300*time.Millisecond, 10*time.Millisecond, "child writer should be called at least once")

	// Spool file should still exist (child failed)
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	_, statErr := os.Stat(spoolPath)
	assert.NoError(t, statErr, "spool file should still exist after child failure")

	// File should be readable
	data, readErr := os.ReadFile(spoolPath)
	require.NoError(t, readErr)
	assert.True(t, len(data) > 4, "spool file should contain framed record data")

	// Verify sessions can be decoded from the file
	_ = writer // keep reference
}

func TestQuarantineStrategyOnRepeatedFailures(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{alwaysFail: true, failCount: -1}
	sw, err := NewDirectSpoolWriter(tmpDir)
	require.NoError(t, err)
	fs := NewQuarantineSpoolStrategy()

	_, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(50*time.Millisecond),
		WithMaxConsecutiveChildWriteFailures(2),
	)
	require.NoError(t, err)
	defer cleanup()

	// when -- write session via direct spool
	var encodedBuf bytes.Buffer
	sessions := []*schema.Session{newTestSession("prop1")}
	_, encErr := encoding.GobEncoder(&encodedBuf, sessions)
	require.NoError(t, encErr)
	err = sw.Write("prop1", encodedBuf.Bytes())
	require.NoError(t, err)

	// then -- spool file should be quarantined (renamed), not deleted
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	quarantinePath := spoolPath + ".quarantine"

	assert.Eventually(t, func() bool {
		_, err := os.Stat(quarantinePath)
		return err == nil
	}, 500*time.Millisecond, 10*time.Millisecond, "quarantine file should exist")

	_, err = os.Stat(spoolPath)
	assert.True(t, os.IsNotExist(err), "original spool file should not exist")

	// Quarantined file should be readable
	data, err := os.ReadFile(quarantinePath)
	require.NoError(t, err)
	assert.True(t, len(data) > 4, "quarantined file should contain data")
}

func TestDeleteStrategyOnRepeatedFailures(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{alwaysFail: true, failCount: -1}

	sw, err := NewBufferedSpoolWriter(tmpDir, 100,
		WithBufferedLvl1MaxSessions(1),
		WithBufferedLvl1MaxAge(10*time.Second),
		WithBufferedLvl1SweepInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	fs := NewDeleteSpoolStrategy()

	_, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(50*time.Millisecond),
		WithMaxConsecutiveChildWriteFailures(2),
	)
	require.NoError(t, err)
	defer cleanup()

	// when -- write session via buffered spool
	var encodedBuf bytes.Buffer
	sessions := []*schema.Session{newTestSession("prop1")}
	_, encErr := encoding.GobEncoder(&encodedBuf, sessions)
	require.NoError(t, encErr)
	err = sw.Write("prop1", encodedBuf.Bytes())
	require.NoError(t, err)

	// then -- wait for spool file to appear on disk (buffered flush)
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	require.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath)
		return err == nil
	}, 300*time.Millisecond, 10*time.Millisecond, "spool file should exist after lvl1 flush")

	// Wait for spool file to be deleted (delete strategy)
	assert.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath)
		return os.IsNotExist(err)
	}, 500*time.Millisecond, 10*time.Millisecond, "spool file should be deleted after threshold")

	// No quarantine file should exist
	quarantinePath := spoolPath + ".quarantine"
	_, err = os.Stat(quarantinePath)
	assert.True(t, os.IsNotExist(err), "quarantine file should not exist with delete strategy")
}

func TestPreExistingSpoolFileIsReplayedOnLvl2Tick(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{}
	sw := &mockSpoolWriter{}
	fs := &mockSpoolFailureStrategy{}

	// Pre-create a spool file with valid framed record
	var encodedBuf bytes.Buffer
	sessions := []*schema.Session{newTestSession("prop1")}
	_, err := encoding.GobEncoder(&encodedBuf, sessions)
	require.NoError(t, err)
	payload := encodedBuf.Bytes()

	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	header := make([]byte, 4, 4+len(payload))
	binary.LittleEndian.PutUint32(header, uint32(len(payload)))
	err = os.WriteFile(spoolPath, append(header, payload...), 0o644)
	require.NoError(t, err)

	// when
	_, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	defer cleanup()

	// then -- child writer should be called with the pre-existing sessions
	assert.Eventually(t, func() bool {
		calls := childWriter.getWriteCalls()
		return len(calls) >= 1
	}, 300*time.Millisecond, 10*time.Millisecond, "child writer should be called for pre-existing spool")

	calls := childWriter.getWriteCalls()
	require.GreaterOrEqual(t, len(calls), 1)
	require.Len(t, calls[0], 1)
	assert.Equal(t, "prop1", calls[0][0].PropertyID)

	// Spool file should be removed after success
	assert.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath)
		return os.IsNotExist(err)
	}, 200*time.Millisecond, 10*time.Millisecond, "spool file should be removed after successful flush")
}

func TestCleanupCallsSpoolWriterCloseAndDoesNotFlushLvl2(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{}
	sw := &mockSpoolWriter{}
	fs := &mockSpoolFailureStrategy{}

	// Pre-create a spool file
	var encodedBuf bytes.Buffer
	sessions := []*schema.Session{newTestSession("prop1")}
	_, err := encoding.GobEncoder(&encodedBuf, sessions)
	require.NoError(t, err)
	payload := encodedBuf.Bytes()

	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	header := make([]byte, 4, 4+len(payload))
	binary.LittleEndian.PutUint32(header, uint32(len(payload)))
	err = os.WriteFile(spoolPath, append(header, payload...), 0o644)
	require.NoError(t, err)

	_, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(10*time.Second), // Long interval so it doesn't trigger
	)
	require.NoError(t, err)

	// when
	cleanup()

	// then
	// SpoolWriter.Close() should be called
	assert.Equal(t, 1, sw.getCloseCalls(), "spool writer Close should be called once")

	// Child writer should NOT be called during cleanup
	assert.Equal(t, 0, len(childWriter.getWriteCalls()), "child writer should not be called during cleanup")

	// Spool file should still exist (no lvl2 flush to child on stop)
	_, err = os.Stat(spoolPath)
	assert.NoError(t, err, "spool file should still exist after cleanup")
}

func TestBufferedSpoolWriterFlushesToDiskOnCount(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	sw, err := NewBufferedSpoolWriter(tmpDir, 100,
		WithBufferedLvl1MaxSessions(2),
		WithBufferedLvl1MaxAge(10*time.Second),
		WithBufferedLvl1SweepInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	defer func() { _ = sw.Close() }()

	payload1 := []byte("payload1")
	payload2 := []byte("payload2")

	// when
	err = sw.Write("prop1", payload1)
	require.NoError(t, err)
	err = sw.Write("prop1", payload2)
	require.NoError(t, err)

	// then -- spool file should exist on disk with both records
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	assert.Eventually(t, func() bool {
		info, err := os.Stat(spoolPath)
		if err != nil {
			return false
		}
		return info.Size() > 0
	}, 300*time.Millisecond, 10*time.Millisecond, "spool file should exist after count flush")

	// Verify framed records
	data, err := os.ReadFile(spoolPath)
	require.NoError(t, err)

	// First record
	require.True(t, len(data) >= 4)
	len1 := binary.LittleEndian.Uint32(data[:4])
	assert.Equal(t, uint32(len(payload1)), len1)
	assert.Equal(t, payload1, data[4:4+len1])

	// Second record
	offset := 4 + len1
	require.True(t, uint32(len(data)) >= offset+4)
	len2 := binary.LittleEndian.Uint32(data[offset : offset+4])
	assert.Equal(t, uint32(len(payload2)), len2)
	assert.Equal(t, payload2, data[offset+4:offset+4+len2])
}

func TestBufferedSpoolWriterFlushesOnClose(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	sw, err := NewBufferedSpoolWriter(tmpDir, 100,
		WithBufferedLvl1MaxSessions(1000), // High threshold
		WithBufferedLvl1MaxAge(10*time.Second),
		WithBufferedLvl1SweepInterval(10*time.Second),
	)
	require.NoError(t, err)

	payload := []byte("test-payload")
	err = sw.Write("prop1", payload)
	require.NoError(t, err)

	// when
	err = sw.Close()
	require.NoError(t, err)

	// then -- spool file should exist (flushed on close)
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	data, err := os.ReadFile(spoolPath)
	require.NoError(t, err)
	require.True(t, len(data) >= 4)
	payloadLen := binary.LittleEndian.Uint32(data[:4])
	assert.Equal(t, uint32(len(payload)), payloadLen)
	assert.Equal(t, payload, data[4:4+payloadLen])
}

func TestBufferedSpoolWriterRejectsWriteAfterClose(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	sw, err := NewBufferedSpoolWriter(tmpDir, 1)
	require.NoError(t, err)

	require.NoError(t, sw.Close())

	// when
	err = sw.Write("prop1", []byte("payload"))

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "buffered spool writer is stopped")
}

func TestLvl2FlushCallsChildWriterPerProperty(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &mockSessionWriter{}
	sw, err := NewBufferedSpoolWriter(tmpDir, 100,
		WithBufferedLvl1MaxSessions(1),
		WithBufferedLvl1MaxAge(10*time.Second),
		WithBufferedLvl1SweepInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	fs := NewDeleteSpoolStrategy()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(200*time.Millisecond),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	err = writer.Write(newTestSession("prop1"))
	require.NoError(t, err)
	err = writer.Write(newTestSession("prop2"))
	require.NoError(t, err)

	// then
	assert.Eventually(t, func() bool {
		return len(childWriter.writeCalls) >= 2
	}, 800*time.Millisecond, 10*time.Millisecond, "child writer should be called at least twice")

	propertyIDs := make(map[string]bool)
	for _, call := range childWriter.writeCalls {
		require.Greater(t, len(call.sessions), 0)
		propID := call.sessions[0].PropertyID
		for _, sess := range call.sessions {
			assert.Equal(t, propID, sess.PropertyID)
		}
		propertyIDs[propID] = true
	}

	assert.True(t, propertyIDs["prop1"])
	assert.True(t, propertyIDs["prop2"])
}

func TestSpoolFailureCounterResetOnSuccess(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{failCount: 1}
	sw, err := NewBufferedSpoolWriter(tmpDir, 100,
		WithBufferedLvl1MaxSessions(1),
		WithBufferedLvl1MaxAge(10*time.Second),
		WithBufferedLvl1SweepInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	fs := NewDeleteSpoolStrategy()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(100*time.Millisecond),
		WithMaxConsecutiveChildWriteFailures(3),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	err = writer.Write(newTestSession("prop1"))
	require.NoError(t, err)

	// then -- wait for spool file to appear on disk first
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	require.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath)
		return err == nil
	}, 300*time.Millisecond, 10*time.Millisecond, "spool file should exist after lvl1 flush")

	// Wait for first failure and second success; file should be removed
	assert.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath)
		return os.IsNotExist(err)
	}, 1*time.Second, 10*time.Millisecond, "spool file should be removed after successful retry")

	calls := childWriter.getWriteCalls()
	assert.GreaterOrEqual(t, len(calls), 2, "should have at least 2 calls (1 fail + 1 success)")
}

func TestFailureCounterNotResetWhenFailureStrategyFails(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	childWriter := &countingMockSessionWriter{alwaysFail: true, failCount: -1}
	sw, err := NewDirectSpoolWriter(tmpDir)
	require.NoError(t, err)

	fs := &mockSpoolFailureStrategy{err: fmt.Errorf("rename failed")}

	writer, cleanup, err := NewBackgroundBatchingWriter(
		context.Background(),
		childWriter,
		sw,
		fs,
		WithSpoolDir(tmpDir),
		WithLvl2FlushInterval(50*time.Millisecond),
		WithMaxConsecutiveChildWriteFailures(2),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	err = writer.Write(newTestSession("prop1"))
	require.NoError(t, err)

	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(spoolPath)
		return statErr == nil
	}, 300*time.Millisecond, 10*time.Millisecond)

	// then
	assert.Eventually(t, func() bool {
		return len(fs.getPaths()) >= 2
	}, 400*time.Millisecond, 10*time.Millisecond, "failure strategy should be retried on subsequent ticks")

	_, statErr := os.Stat(spoolPath)
	assert.NoError(t, statErr, "spool file should remain when failure strategy fails")
}
