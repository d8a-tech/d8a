package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestGetServerFlags_IncludeDeliveryModeAndReceiverBatchingBackend(t *testing.T) {
	setDeliveryModeForTest(t, "")

	// given
	flags := getServerFlags()

	// when
	names := make([]string, 0, len(flags))
	for _, flag := range flags {
		names = append(names, flag.Names()[0])
	}

	// then
	assert.True(t, slices.Contains(names, deliveryModeFlag.Name))
	assert.True(t, slices.Contains(names, receiverBatchingBackendFlag.Name))
}

func TestDeliveryModeFlags_DefaultValues(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")

	unsetEnvForTest(t, "DELIVERY_MODE")
	unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")
	unsetEnvForTest(t, "STORAGE_SPOOL_ENABLED")
	unsetEnvForTest(t, "STORAGE_SPOOL_WRITE_CHAN_BUFFER")

	args := []string{"d8a-test"}
	setCurrentRunArgsForTest(t, args)

	var observedDeliveryMode string
	var observedReceiverBatchingBackend string

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(_ context.Context, cmd *cli.Command) error {
			// when
			observedDeliveryMode = cmd.String(deliveryModeFlag.Name)
			observedReceiverBatchingBackend = cmd.String(receiverBatchingBackendFlag.Name)
			return nil
		},
	}

	// then
	require.NoError(t, app.Run(context.Background(), args))
	assert.Equal(t, "best_effort", observedDeliveryMode)
	assert.Equal(t, "memory", observedReceiverBatchingBackend)
}

func TestDeliveryModeOverrides_WhenAtLeastOnceFromConfig(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")

	unsetEnvForTest(t, "DELIVERY_MODE")
	unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")
	unsetEnvForTest(t, "STORAGE_SPOOL_ENABLED")
	unsetEnvForTest(t, "STORAGE_SPOOL_WRITE_CHAN_BUFFER")

	configPath := writeConfigFile(t, `
delivery:
  mode: at_least_once
receiver:
  batching_backend: memory
storage:
  spool_enabled: false
  spool_write_chan_buffer: 99
`)
	setConfigFileForTest(t, configPath)

	args := []string{"d8a-test", "--config=" + configPath}
	setCurrentRunArgsForTest(t, args)

	logs := captureWarnLogs(t)

	var observedReceiverBatchingBackend string
	var observedStorageSpoolEnabled bool
	var observedStorageSpoolWriteChanBuffer int

	app := &cli.Command{
		Name:   "d8a-test",
		Before: applyDeliveryModeOverridesBefore,
		Flags:  mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(_ context.Context, cmd *cli.Command) error {
			// when
			observedReceiverBatchingBackend = cmd.String(receiverBatchingBackendFlag.Name)
			observedStorageSpoolEnabled = cmd.Bool(storageSpoolEnabledFlag.Name)
			observedStorageSpoolWriteChanBuffer = cmd.Int(storageSpoolWriteChanBufferFlag.Name)
			return nil
		},
	}

	// then
	require.NoError(t, app.Run(context.Background(), args))
	assert.Equal(t, "filesystem", observedReceiverBatchingBackend)
	assert.Equal(t, true, observedStorageSpoolEnabled)
	assert.Equal(t, 0, observedStorageSpoolWriteChanBuffer)
	assert.Contains(t, logs.String(), "delivery mode 'at_least_once' sets 'receiver-batching-backend' to 'filesystem'")
	assert.Contains(t, logs.String(), "delivery mode 'at_least_once' sets 'storage-spool-enabled' to 'true'")
	assert.Contains(t, logs.String(), "delivery mode 'at_least_once' sets 'storage-spool-write-chan-buffer' to '0'")
}

func TestDeliveryModeOverrides_WarningsOnlyForConflictingNonCLIValues(t *testing.T) {
	testCases := []struct {
		name                string
		configContent       string
		args                []string
		expectedWarningPart string
		wantWarning         bool
	}{
		{
			name: "no warning when CLI explicitly sets forced values",
			configContent: `
delivery:
  mode: at_least_once
receiver:
  batching_backend: memory
storage:
  spool_enabled: false
  spool_write_chan_buffer: 17
`,
			args: []string{
				"d8a-test",
				"--receiver-batching-backend=filesystem",
				"--storage-spool-enabled=true",
				"--storage-spool-write-chan-buffer=0",
			},
			expectedWarningPart: "delivery mode 'at_least_once' sets",
			wantWarning:         false,
		},
		{
			name: "warning when config conflicts and not explicitly forced in CLI",
			configContent: `
delivery:
  mode: at_least_once
receiver:
  batching_backend: memory
storage:
  spool_enabled: false
  spool_write_chan_buffer: 17
`,
			args:                []string{"d8a-test"},
			expectedWarningPart: "delivery mode 'at_least_once' sets",
			wantWarning:         true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// given
			setDeliveryModeForTest(t, "")

			unsetEnvForTest(t, "DELIVERY_MODE")
			unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")
			unsetEnvForTest(t, "STORAGE_SPOOL_ENABLED")
			unsetEnvForTest(t, "STORAGE_SPOOL_WRITE_CHAN_BUFFER")

			configPath := writeConfigFile(t, testCase.configContent)
			setConfigFileForTest(t, configPath)
			args := append([]string(nil), testCase.args...)
			args = append(args, "--config="+configPath)
			setCurrentRunArgsForTest(t, args)

			logs := captureWarnLogs(t)

			app := &cli.Command{
				Name:   "d8a-test",
				Before: applyDeliveryModeOverridesBefore,
				Flags:  mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
				Action: func(_ context.Context, _ *cli.Command) error {
					// when
					return nil
				},
			}

			// then
			require.NoError(t, app.Run(context.Background(), args))
			if testCase.wantWarning {
				assert.Contains(t, logs.String(), testCase.expectedWarningPart)
				return
			}

			assert.NotContains(t, logs.String(), testCase.expectedWarningPart)
		})
	}
}

func TestDeliveryModeDetection_CLIArgPrecedenceOverEnv(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")

	unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")
	unsetEnvForTest(t, "STORAGE_SPOOL_ENABLED")
	unsetEnvForTest(t, "STORAGE_SPOOL_WRITE_CHAN_BUFFER")

	t.Setenv("DELIVERY_MODE", "at_least_once")
	configPath := writeConfigFile(t, "")
	setConfigFileForTest(t, configPath)

	args := []string{"d8a-test", "--config=" + configPath, "--delivery-mode=best_effort"}
	setCurrentRunArgsForTest(t, args)

	var observedReceiverBatchingBackend string
	var observedStorageSpoolEnabled bool
	var observedStorageSpoolWriteChanBuffer int

	app := &cli.Command{
		Name:   "d8a-test",
		Before: applyDeliveryModeOverridesBefore,
		Flags:  mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(_ context.Context, cmd *cli.Command) error {
			// when
			observedReceiverBatchingBackend = cmd.String(receiverBatchingBackendFlag.Name)
			observedStorageSpoolEnabled = cmd.Bool(storageSpoolEnabledFlag.Name)
			observedStorageSpoolWriteChanBuffer = cmd.Int(storageSpoolWriteChanBufferFlag.Name)
			return nil
		},
	}

	// then
	require.NoError(t, app.Run(context.Background(), args))
	assert.Equal(t, "memory", observedReceiverBatchingBackend)
	assert.Equal(t, true, observedStorageSpoolEnabled)
	assert.Equal(t, 1000, observedStorageSpoolWriteChanBuffer)
}

func TestRunSubcommands_UseCombinedModeOverridesBeforeHook(t *testing.T) {
	setDeliveryModeForTest(t, "")

	// given
	_, currentFilePath, _, ok := runtime.Caller(0)
	require.True(t, ok)

	runGoPath := filepath.Join(filepath.Dir(currentFilePath), "run.go")
	runGoContent, err := os.ReadFile(runGoPath)
	require.NoError(t, err)

	testCases := []string{"columns", "server", "receiver", "worker", "migrate"}

	for _, commandName := range testCases {
		// when
		pattern := regexp.MustCompile(
			`(?s)Name:\s+"` + regexp.QuoteMeta(commandName) +
				`".*?Before:\s+applyModeOverridesBefore`,
		)

		// then
		assert.True(t, pattern.Match(runGoContent))
	}
	assert.Contains(t, string(runGoContent), "applyAirgappedOverridesBefore")
	assert.Contains(t, string(runGoContent), "applyDeliveryModeOverridesBefore")
}

func TestDeliveryModeOverrides_SkipsUndefinedFlagsOnCommand(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")

	unsetEnvForTest(t, "DELIVERY_MODE")
	unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")
	unsetEnvForTest(t, "STORAGE_SPOOL_ENABLED")
	unsetEnvForTest(t, "STORAGE_SPOOL_WRITE_CHAN_BUFFER")

	configPath := writeConfigFile(t, `
delivery:
  mode: at_least_once
receiver:
  batching_backend: memory
storage:
  spool_enabled: false
  spool_write_chan_buffer: 99
`)
	setConfigFileForTest(t, configPath)

	args := []string{"migrate-test", "--property-id=test", "--config=" + configPath}
	setCurrentRunArgsForTest(t, args)

	// Command without receiver/storage flags that are normally overridden by delivery mode.
	// This simulates the migrate command which doesn't expose those flags.
	app := &cli.Command{
		Name:   "migrate-test",
		Before: applyDeliveryModeOverridesBefore,
		Flags: mergeFlags(
			[]cli.Flag{
				configFlag,
				&cli.StringFlag{
					Name:     "property-id",
					Required: true,
				},
			},
			// Deliberately omit receiver/storage flags
		),
		Action: func(_ context.Context, _ *cli.Command) error {
			return nil
		},
	}

	// when / then
	// Should not fail even though delivery-mode=at_least_once is enabled
	// and the command doesn't have receiver-batching-backend or storage-spool-enabled flags
	require.NoError(t, app.Run(context.Background(), args))
}

func TestDeliveryModeOverrides_AppliesWhenFlagsExist(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")

	unsetEnvForTest(t, "DELIVERY_MODE")
	unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")
	unsetEnvForTest(t, "STORAGE_SPOOL_ENABLED")
	unsetEnvForTest(t, "STORAGE_SPOOL_WRITE_CHAN_BUFFER")

	configPath := writeConfigFile(t, `
delivery:
  mode: at_least_once
receiver:
  batching_backend: memory
storage:
  spool_enabled: false
  spool_write_chan_buffer: 99
`)
	setConfigFileForTest(t, configPath)

	args := []string{"server-test", "--config=" + configPath}
	setCurrentRunArgsForTest(t, args)

	var observedReceiverBatchingBackend string
	var observedStorageSpoolEnabled bool
	var observedStorageSpoolWriteChanBuffer int

	// Command with all receiver/storage flags. This simulates server/receiver/worker commands.
	app := &cli.Command{
		Name:   "server-test",
		Before: applyDeliveryModeOverridesBefore,
		Flags: mergeFlags(
			[]cli.Flag{configFlag},
			getServerFlags(),
		),
		Action: func(_ context.Context, cmd *cli.Command) error {
			// when
			observedReceiverBatchingBackend = cmd.String(receiverBatchingBackendFlag.Name)
			observedStorageSpoolEnabled = cmd.Bool(storageSpoolEnabledFlag.Name)
			observedStorageSpoolWriteChanBuffer = cmd.Int(storageSpoolWriteChanBufferFlag.Name)
			return nil
		},
	}

	// then
	require.NoError(t, app.Run(context.Background(), args))
	assert.Equal(t, "filesystem", observedReceiverBatchingBackend)
	assert.Equal(t, true, observedStorageSpoolEnabled)
	assert.Equal(t, 0, observedStorageSpoolWriteChanBuffer)
}

func TestDeliveryModeOverrideSources_CanReadConfigValues(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")

	unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")
	unsetEnvForTest(t, "STORAGE_SPOOL_WRITE_CHAN_BUFFER")

	configPath := writeConfigFile(t, `
receiver:
  batching_backend: filesystem
storage:
  spool_write_chan_buffer: 0
`)
	setConfigFileForTest(t, configPath)

	// when
	batchingSource := defaultSourceChain(
		"RECEIVER_BATCHING_BACKEND",
		"receiver.batching_backend",
	)
	batchingValue, batchingFound := batchingSource.Lookup()

	bufferSource := defaultSourceChain(
		"STORAGE_SPOOL_WRITE_CHAN_BUFFER",
		"storage.spool_write_chan_buffer",
	)
	bufferValue, bufferFound := bufferSource.Lookup()

	// then
	assert.True(t, batchingFound)
	assert.Equal(t, "filesystem", batchingValue)
	assert.True(t, bufferFound)
	assert.Equal(t, "0", bufferValue)
}

func setCurrentRunArgsForTest(t *testing.T, appArgs []string) {
	t.Helper()

	if len(appArgs) == 0 {
		currentRunArgs = nil
	} else {
		currentRunArgs = append([]string(nil), appArgs[1:]...)
	}

	t.Cleanup(func() {
		currentRunArgs = nil
	})
}

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func setConfigFileForTest(t *testing.T, path string) {
	t.Helper()

	previous := configFile
	configFile = path

	t.Cleanup(func() {
		configFile = previous
	})
}

func setDeliveryModeForTest(t *testing.T, value string) {
	t.Helper()

	previous := deliveryMode
	deliveryMode = value

	t.Cleanup(func() {
		deliveryMode = previous
	})
}

func captureWarnLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	buffer := &bytes.Buffer{}
	logger := logrus.StandardLogger()
	previousOut := logger.Out
	previousLevel := logger.GetLevel()

	logger.SetOutput(buffer)
	logger.SetLevel(logrus.WarnLevel)

	t.Cleanup(func() {
		logger.SetOutput(previousOut)
		logger.SetLevel(previousLevel)
	})

	return buffer
}

func unsetEnvForTest(t *testing.T, key string) {
	t.Helper()

	value, exists := os.LookupEnv(key)
	require.NoError(t, os.Unsetenv(key))

	t.Cleanup(func() {
		if !exists {
			require.NoError(t, os.Unsetenv(key))
			return
		}

		require.NoError(t, os.Setenv(key, value))
	})
}
