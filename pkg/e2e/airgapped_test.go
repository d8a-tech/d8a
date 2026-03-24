package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAirgappedModeOverridesConflictingSettings(t *testing.T) {
	const port = 17041

	binaryPath := buildBinary(t)
	t.Cleanup(func() { _ = os.Remove(binaryPath) })

	tmpDir := t.TempDir()
	queueDir := filepath.Join(tmpDir, "queue")
	storageDir := filepath.Join(tmpDir, "storage")
	require.NoError(t, os.MkdirAll(queueDir, 0o755))
	require.NoError(t, os.MkdirAll(storageDir, 0o755))

	configPath := filepath.Join(tmpDir, "config.yaml")
	configContent := fmt.Sprintf(`airgapped: true
warehouse: noop

receiver:
  batch_size: 100
  batch_timeout: 100ms

sessions:
  timeout: 2s

dbip:
  enabled: true

currency:
  refresh_interval: 1s

telemetry:
  url: "https://global.t.d8a.tech/test"

monitoring:
  enabled: false

storage:
  bolt_directory: %s/
  queue_directory: %s
  spool_enabled: false

server:
  port: %d

property:
  id: test-property
  name: Test Property
`, storageDir, queueDir, port)
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0o600))

	handle, err := startProcessInBackground(t, binaryPath, "server", "--config", configPath)
	require.NoError(t, err)

	require.True(t, waitForServerReady(port, 10*time.Second), "server should become ready")
	require.True(
		t,
		handle.logs.waitFor("airgapped mode sets 'dbip-enabled' to 'false'", 5*time.Second),
		"dbip warning should be logged",
	)
	require.True(
		t,
		handle.logs.waitFor("airgapped mode sets 'currency-refresh-interval' to '0'", 5*time.Second),
		"currency warning should be logged",
	)
	require.True(
		t,
		handle.logs.waitFor("airgapped mode sets 'telemetry-url' to ''", 5*time.Second),
		"telemetry warning should be logged",
	)
}
