package e2e

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSeparateReceiverAndWorker validates that hits flow from receiver through
// filesystem queue to worker when running as separate processes
func TestSeparateReceiverAndWorker(t *testing.T) {
	const port = 17032

	// given - setup
	// Build binary once
	binaryPath := buildBinary(t)
	t.Cleanup(func() { _ = os.Remove(binaryPath) })

	// Create shared temp directories (queue, storage)
	queueDir, storageDir := buildSharedTempDirectories(t)

	// Generate configs for receiver and worker
	receiverConfigPath := newTestConfigBuilder().
		WithPort(port).
		WithQueueDirectory(queueDir).
		WithStorageDirectory(storageDir).
		WithWarehouse("noop").
		WithSessionTimeout(2 * time.Second).
		Build(t)

	workerConfigPath := newTestConfigBuilder().
		WithQueueDirectory(queueDir).
		WithStorageDirectory(storageDir).
		WithWarehouse("noop").
		WithSessionTimeout(2 * time.Second).
		Build(t)

	// when - execution
	// Start worker process in background
	workerHandle, err := startProcessInBackground(t, binaryPath, "worker", "--config", workerConfigPath)
	require.NoError(t, err, "failed to start worker process")

	// Start receiver process in background
	receiverHandle, err := startProcessInBackground(t, binaryPath, "receiver", "--config", receiverConfigPath)
	require.NoError(t, err, "failed to start receiver process")

	// Wait for receiver to be ready (healthz endpoint)
	require.True(t, waitForServerReady(port, 10*time.Second), "receiver should become ready")

	// Send GA4 tracking request sequence (3 hits from existing test)
	striker := NewGA4RequestGenerator("localhost", port)
	err = striker.Replay([]HitSequenceItem{
		{
			ClientID:     "client-1",
			EventType:    "page_view",
			SessionStamp: "127.0.0.1",
			Description:  "client 1",
			SleepBefore:  0,
		},
		{
			ClientID:     "client-2",
			EventType:    "scroll",
			SessionStamp: "127.0.0.2",
			Description:  "client 2",
			SleepBefore:  100 * time.Millisecond,
		},
		{
			ClientID:     "client-3",
			EventType:    "page_view",
			SessionStamp: "127.0.0.1",
			Description:  "client 3 (should be same session as client 1)",
			SleepBefore:  100 * time.Millisecond,
		},
	})
	require.NoError(t, err, "failed to replay GA4 sequence")

	// then - verification
	// Verify receiver started successfully
	assert.True(
		t,
		receiverHandle.logs.waitFor("Starting server", 5*time.Second),
		"receiver should start successfully",
	)

	// Verify worker logs show "Appending to proto-sessions" messages (worker processes hits)
	assert.True(
		t,
		workerHandle.logs.waitFor("Appending to proto-sessions", 5*time.Second),
		"worker should process hits from queue",
	)

	// Verify worker logs show "flushing batch" to warehouse
	assert.True(
		t,
		workerHandle.logs.waitFor("flushing batch", 10*time.Second),
		"worker should flush batch to warehouse",
	)

	// Verify no error logs from either process
	receiverLines := receiverHandle.logs.GetLines()
	for _, line := range receiverLines {
		assert.NotContains(t, line, "level=error", "receiver should not log errors")
		assert.NotContains(t, line, "level=fatal", "receiver should not log fatal errors")
	}

	workerLines := workerHandle.logs.GetLines()
	for _, line := range workerLines {
		assert.NotContains(t, line, "level=error", "worker should not log errors")
		assert.NotContains(t, line, "level=fatal", "worker should not log fatal errors")
	}

	// Both processes shut down cleanly (cleanup happens automatically via t.Cleanup)
}
