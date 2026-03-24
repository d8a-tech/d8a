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

	tmpDir := t.TempDir()

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
  queue_directory: %s/queue
  spool_enabled: false

server:
  port: %d

property:
  id: test-property
  name: Test Property
`, dockerSharedStoragePath, dockerSharedStoragePath, port)
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0o644))

	handle, err := startDockerProcessInBackground(
		t,
		defaultDockerServerRunOptions(configPath, port, withDockerNetworkMode("none")),
	)
	require.NoError(t, err)

	require.True(t, handle.logs.waitFor("starting server", 10*time.Second), "server should become ready")
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

	striker := NewGA4RequestGenerator("localhost", port)
	replayViaLocalfetch(t, handle, port, striker, []HitSequenceItem{
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

	require.True(
		t,
		handle.logs.waitFor("flushing batch of size", 10*time.Second),
		"airgapped mode should still process local tracking hits",
	)
}

func replayViaLocalfetch(
	t *testing.T,
	handle *processHandle,
	port int,
	striker *GA4RequestGenerator,
	sequence []HitSequenceItem,
) {
	t.Helper()

	for _, hit := range sequence {
		time.Sleep(hit.SleepBefore)

		queryString := striker.QueryString(hit.ClientID, hit.EventType, hit.SessionStamp)
		output, err := handle.dockerExec(
			"/bin/app",
			"localfetch",
			"--port",
			fmt.Sprintf("%d", port),
			"--path",
			"/g/collect",
			"--query-string",
			queryString,
		)
		require.NoError(t, err, "failed to send hit %s: %s", hit.Description, string(output))
	}
}
