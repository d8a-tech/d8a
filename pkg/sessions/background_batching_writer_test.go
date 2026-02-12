package sessions

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingMockSessionWriter is a mock that fails a specific number of times before succeeding.
// If failCount is negative, it always fails.
type countingMockSessionWriter struct {
	writeCalls      [][]*schema.Session
	failCount       int
	currentFailures int
	alwaysFail      bool
}

func (m *countingMockSessionWriter) Write(sessions ...*schema.Session) error {
	m.writeCalls = append(m.writeCalls, sessions)
	if m.alwaysFail || (m.failCount >= 0 && m.currentFailures < m.failCount) {
		m.currentFailures++
		return assert.AnError
	}
	return nil
}

// newTestSession creates a test session with a given property ID.
func newTestSession(propertyID string) *schema.Session {
	hit := &hits.Hit{
		PropertyID: propertyID,
	}
	event := schema.NewEvent(hit)
	return schema.NewSession([]*schema.Event{event})
}

func TestLvl1BatchesPerPropertyAndFlushesOnCount(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	mockWriter := &mockSessionWriter{}
	ctx := context.Background()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		ctx,
		mockWriter,
		WithSpoolDir(tmpDir),
		WithLvl1MaxSessions(3),
		WithLvl1MaxAge(10*time.Second), // Long enough to not trigger
		WithLvl1SweepInterval(100*time.Millisecond),
		WithLvl2FlushInterval(10*time.Second), // Long enough to not trigger
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	// Write 5 sessions for property1 (should trigger flush at 3, then 2 remain)
	sessions1 := []*schema.Session{
		newTestSession("prop1"),
		newTestSession("prop1"),
		newTestSession("prop1"),
	}
	err = writer.Write(sessions1...)
	require.NoError(t, err)

	// Write 2 more (should trigger another flush)
	sessions2 := []*schema.Session{
		newTestSession("prop1"),
		newTestSession("prop1"),
	}
	err = writer.Write(sessions2...)
	require.NoError(t, err)

	// Wait for spool file to exist and have content
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	assert.Eventually(t, func() bool {
		fileInfo, err := os.Stat(spoolPath)
		if err != nil {
			return false
		}
		return fileInfo.Size() > 0
	}, 500*time.Millisecond, 10*time.Millisecond, "spool file should contain data")
}

func TestLvl1FlushesOnAge(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	mockWriter := &mockSessionWriter{}
	ctx := context.Background()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		ctx,
		mockWriter,
		WithSpoolDir(tmpDir),
		WithLvl1MaxSessions(1000), // High enough to not trigger
		WithLvl1MaxAge(100*time.Millisecond),
		WithLvl1SweepInterval(50*time.Millisecond),
		WithLvl2FlushInterval(10*time.Second), // Long enough to not trigger
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	session := newTestSession("prop1")
	err = writer.Write(session)
	require.NoError(t, err)

	// then
	// Wait for age-based flush
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	assert.Eventually(t, func() bool {
		fileInfo, err := os.Stat(spoolPath)
		if err != nil {
			return false
		}
		return fileInfo.Size() > 0
	}, 300*time.Millisecond, 10*time.Millisecond, "spool file should be created and contain data")
}

func TestLvl2FlushCallsChildWriterPerProperty(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	mockWriter := &mockSessionWriter{}
	ctx := context.Background()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		ctx,
		mockWriter,
		WithSpoolDir(tmpDir),
		WithLvl1MaxSessions(1), // Flush immediately
		WithLvl1MaxAge(10*time.Second),
		WithLvl1SweepInterval(100*time.Millisecond),
		WithLvl2FlushInterval(200*time.Millisecond), // Short interval for testing
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	// Write sessions for two different properties
	sessions1 := []*schema.Session{newTestSession("prop1")}
	err = writer.Write(sessions1...)
	require.NoError(t, err)

	sessions2 := []*schema.Session{newTestSession("prop2")}
	err = writer.Write(sessions2...)
	require.NoError(t, err)

	// then
	// Wait for child writer to be called twice (once per property)
	assert.Eventually(t, func() bool {
		return len(mockWriter.writeCalls) >= 2
	}, 500*time.Millisecond, 10*time.Millisecond, "child writer should be called at least twice")

	// Verify each call contains sessions for only one property
	propertyIDs := make(map[string]bool)
	for _, call := range mockWriter.writeCalls {
		require.Greater(t, len(call.sessions), 0, "each call should have at least one session")
		propID := call.sessions[0].PropertyID
		for _, sess := range call.sessions {
			assert.Equal(t, propID, sess.PropertyID, "all sessions in a call should have the same property ID")
		}
		propertyIDs[propID] = true
	}

	assert.True(t, propertyIDs["prop1"], "prop1 should be flushed")
	assert.True(t, propertyIDs["prop2"], "prop2 should be flushed")

	// Spool files should be removed after successful flush
	spoolPath1 := filepath.Join(tmpDir, "property_prop1.spool")
	spoolPath2 := filepath.Join(tmpDir, "property_prop2.spool")
	_, err1 := os.Stat(spoolPath1)
	_, err2 := os.Stat(spoolPath2)
	assert.True(t, os.IsNotExist(err1), "prop1 spool file should be removed after flush")
	assert.True(t, os.IsNotExist(err2), "prop2 spool file should be removed after flush")
}

func TestLvl2FlushKeepsSpoolOnError(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	mockWriter := &mockSessionWriter{
		writeError: assert.AnError, // Simulate error
	}
	ctx := context.Background()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		ctx,
		mockWriter,
		WithSpoolDir(tmpDir),
		WithLvl1MaxSessions(1), // Flush immediately
		WithLvl1MaxAge(10*time.Second),
		WithLvl1SweepInterval(100*time.Millisecond),
		WithLvl2FlushInterval(200*time.Millisecond),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	session := newTestSession("prop1")
	err = writer.Write(session)
	require.NoError(t, err)

	// then
	// Wait for spool file to exist after lvl1 flush
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	assert.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath)
		return err == nil
	}, 200*time.Millisecond, 10*time.Millisecond, "spool file should exist after lvl1 flush")

	// Wait for lvl2 flush interval to pass (200ms) plus some buffer
	// The file should still exist since child writer always errors
	assert.Eventually(t, func() bool {
		// File should still exist after flush attempt
		_, err := os.Stat(spoolPath)
		return err == nil
	}, 400*time.Millisecond, 10*time.Millisecond, "spool file should be kept on child writer error")
}

func TestCleanupFlushesLvl1ToLvl2AndStops(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	mockWriter := &mockSessionWriter{}
	ctx := context.Background()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		ctx,
		mockWriter,
		WithSpoolDir(tmpDir),
		WithLvl1MaxSessions(1000), // High enough to not auto-flush
		WithLvl1MaxAge(10*time.Second),
		WithLvl1SweepInterval(100*time.Millisecond),
		WithLvl2FlushInterval(10*time.Second), // Long enough to not auto-flush
	)
	require.NoError(t, err)

	// Write some sessions (will stay in lvl1)
	sessions1 := []*schema.Session{
		newTestSession("prop1"),
		newTestSession("prop1"),
	}
	err = writer.Write(sessions1...)
	require.NoError(t, err)

	// Give the actor time to receive from writeChan and add to lvl1 (Write is fire-and-forget)
	time.Sleep(100 * time.Millisecond)

	// when
	// Call cleanup (should flush lvl1 to lvl2, but NOT lvl2 to child)
	cleanup()

	// then
	// Child writer should NOT be called during cleanup (we don't flush lvl2 to child)
	assert.Equal(t, 0, len(mockWriter.writeCalls), "child writer should not be called during cleanup")

	// Spool file should exist (lvl1 was flushed to lvl2)
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	assert.Eventually(t, func() bool {
		fileInfo, err := os.Stat(spoolPath)
		if err != nil {
			return false
		}
		return fileInfo.Size() > 0
	}, 200*time.Millisecond, 10*time.Millisecond, "spool file should exist with lvl1 data flushed to lvl2")
}

func TestFramedRecordFormat(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	mockWriter := &mockSessionWriter{}
	ctx := context.Background()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		ctx,
		mockWriter,
		WithSpoolDir(tmpDir),
		WithLvl1MaxSessions(1), // Flush immediately
		WithLvl1MaxAge(10*time.Second),
		WithLvl1SweepInterval(100*time.Millisecond),
		WithLvl2FlushInterval(200*time.Millisecond),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	session := newTestSession("prop1")
	err = writer.Write(session)
	require.NoError(t, err)

	// Wait for lvl1 flush and read spool file
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	var file *os.File
	assert.Eventually(t, func() bool {
		var err error
		file, err = os.Open(spoolPath)
		return err == nil
	}, 200*time.Millisecond, 10*time.Millisecond, "spool file should exist")
	require.NoError(t, err)
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			logrus.Errorf("failed to close test file: %v", closeErr)
		}
	}()

	// Read header
	header := make([]byte, 4)
	n, err := file.Read(header)
	require.NoError(t, err)
	require.Equal(t, 4, n, "should read 4-byte header")

	payloadLen := binary.LittleEndian.Uint32(header)

	// Read payload
	payload := make([]byte, payloadLen)
	n, err = file.Read(payload)
	require.NoError(t, err)
	require.Equal(t, int(payloadLen), n, "should read full payload")

	// Decode and verify
	var decodedSessions []*schema.Session
	err = encoding.GobDecoder(bytes.NewReader(payload), &decodedSessions)
	require.NoError(t, err)
	require.Len(t, decodedSessions, 1, "should decode one session")
	assert.Equal(t, "prop1", decodedSessions[0].PropertyID, "decoded session should have correct property ID")
}

func TestMultiplePropertiesIndependentBatching(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	mockWriter := &mockSessionWriter{}
	ctx := context.Background()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		ctx,
		mockWriter,
		WithSpoolDir(tmpDir),
		WithLvl1MaxSessions(3),
		WithLvl1MaxAge(10*time.Second),
		WithLvl1SweepInterval(100*time.Millisecond),
		WithLvl2FlushInterval(10*time.Second),
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	// Write 2 sessions for prop1 (should not flush)
	sessions1 := []*schema.Session{
		newTestSession("prop1"),
		newTestSession("prop1"),
	}
	err = writer.Write(sessions1...)
	require.NoError(t, err)

	// Write 3 sessions for prop2 (should flush)
	sessions2 := []*schema.Session{
		newTestSession("prop2"),
		newTestSession("prop2"),
		newTestSession("prop2"),
	}
	err = writer.Write(sessions2...)
	require.NoError(t, err)

	// then
	// prop2 spool file should exist (flushed)
	spoolPath2 := filepath.Join(tmpDir, "property_prop2.spool")
	assert.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath2)
		return err == nil
	}, 200*time.Millisecond, 10*time.Millisecond, "prop2 spool file should exist")

	// prop1 spool file should not exist yet (not flushed)
	spoolPath1 := filepath.Join(tmpDir, "property_prop1.spool")
	_, err = os.Stat(spoolPath1)
	assert.True(t, os.IsNotExist(err), "prop1 spool file should not exist yet")
}

func TestSpoolDiscardAfterThreshold(t *testing.T) {
	// given
	testCases := []struct {
		name            string
		threshold       int
		failCount       int
		waitTime        time.Duration
		expectDiscarded bool
		expectCalls     int
	}{
		{
			name:            "discard after threshold reached",
			threshold:       2,
			failCount:       3,                      // Fail more than threshold
			waitTime:        500 * time.Millisecond, // Enough for 2+ flush attempts
			expectDiscarded: true,
			expectCalls:     2, // Should be called threshold times before discard
		},
		{
			name:            "keep spool when failures below threshold",
			threshold:       5,                      // Higher threshold
			failCount:       -1,                     // Always fail (negative means always fail)
			waitTime:        120 * time.Millisecond, // Only wait for ~2 attempts (below threshold of 5)
			expectDiscarded: false,
			expectCalls:     2, // Should be called at least 2 times but below threshold
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			tmpDir := t.TempDir()
			mockWriter := &countingMockSessionWriter{
				failCount:  tc.failCount,
				alwaysFail: tc.failCount < 0,
			}
			ctx := context.Background()

			writer, cleanup, err := NewBackgroundBatchingWriter(
				ctx,
				mockWriter,
				WithSpoolDir(tmpDir),
				WithLvl1MaxSessions(1), // Flush immediately
				WithLvl1MaxAge(10*time.Second),
				WithLvl1SweepInterval(100*time.Millisecond),
				WithLvl2FlushInterval(50*time.Millisecond), // Fast retries
				WithMaxConsecutiveChildWriteFailures(tc.threshold),
			)
			require.NoError(t, err)
			defer cleanup()

			// when
			session := newTestSession("prop1")
			err = writer.Write(session)
			require.NoError(t, err)

			// Wait for lvl1 flush to disk
			spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
			require.Eventually(t, func() bool {
				_, err := os.Stat(spoolPath)
				return err == nil
			}, 200*time.Millisecond, 10*time.Millisecond,
				"spool file should exist after lvl1 flush")

			// then
			if tc.expectDiscarded {
				// Wait for spool file to be discarded
				assert.Eventually(t, func() bool {
					_, err := os.Stat(spoolPath)
					return os.IsNotExist(err)
				}, tc.waitTime, 10*time.Millisecond, "spool file should be discarded after threshold")
				assert.GreaterOrEqual(t, len(mockWriter.writeCalls), tc.expectCalls,
					"child writer should be called at least threshold times")
			} else {
				// Wait a bit for flush attempts, but file should still exist
				time.Sleep(tc.waitTime)
				_, err = os.Stat(spoolPath)
				assert.NoError(t, err, "spool file should still exist when failures below threshold")
				assert.GreaterOrEqual(t, len(mockWriter.writeCalls), tc.expectCalls, "child writer should be called")
			}
		})
	}
}

func TestSpoolFailureCounterResetOnSuccess(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	// Mock that fails once, then succeeds
	mockWriter := &countingMockSessionWriter{
		failCount: 1, // Fail once, then succeed
	}
	ctx := context.Background()

	writer, cleanup, err := NewBackgroundBatchingWriter(
		ctx,
		mockWriter,
		WithSpoolDir(tmpDir),
		WithLvl1MaxSessions(1), // Flush immediately
		WithLvl1MaxAge(10*time.Second),
		WithLvl1SweepInterval(100*time.Millisecond),
		WithLvl2FlushInterval(200*time.Millisecond), // Longer interval to control timing
		WithMaxConsecutiveChildWriteFailures(2),     // Threshold higher than failCount
	)
	require.NoError(t, err)
	defer cleanup()

	// when
	session := newTestSession("prop1")
	err = writer.Write(session)
	require.NoError(t, err)

	// Wait for lvl1 flush to disk
	spoolPath := filepath.Join(tmpDir, "property_prop1.spool")
	assert.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath)
		return err == nil
	}, 200*time.Millisecond, 10*time.Millisecond, "spool file should exist after lvl1 flush")

	// Wait for first lvl2 flush attempt (will fail)
	assert.Eventually(t, func() bool {
		return len(mockWriter.writeCalls) >= 1
	}, 300*time.Millisecond, 10*time.Millisecond, "child writer should be called once (first failure)")

	// Verify spool file still exists after first failure
	_, err = os.Stat(spoolPath)
	require.NoError(t, err, "spool file should exist after first failure")

	// Wait for second lvl2 flush attempt (will succeed, resetting counter)
	assert.Eventually(t, func() bool {
		return len(mockWriter.writeCalls) >= 2
	}, 300*time.Millisecond, 10*time.Millisecond, "child writer should be called twice")

	// then
	// Spool file should be removed after successful flush
	assert.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath)
		return os.IsNotExist(err)
	}, 100*time.Millisecond, 10*time.Millisecond, "spool file should be removed after successful flush")

	// Write another session to verify counter was reset (should succeed immediately)
	session2 := newTestSession("prop1")
	err = writer.Write(session2)
	require.NoError(t, err)

	// Wait for lvl1 flush
	spoolPath2 := filepath.Join(tmpDir, "property_prop1.spool")
	assert.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath2)
		return err == nil
	}, 200*time.Millisecond, 10*time.Millisecond, "new spool file should exist after lvl1 flush")

	// Wait for lvl2 flush (should succeed immediately since counter was reset)
	assert.Eventually(t, func() bool {
		_, err := os.Stat(spoolPath2)
		return os.IsNotExist(err)
	}, 300*time.Millisecond, 10*time.Millisecond, "new spool file should be removed immediately after successful flush")
}
