package cmd

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

type noopPublisher struct{}

func (n *noopPublisher) Publish(_ *worker.Task) error { return nil }

func TestBuildReceiverStorage_DefaultUsesMemoryBackend(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")
	unsetEnvForTest(t, "DELIVERY_MODE")
	unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")

	var capturedOpts []receiver.BatchingOption
	origFn := newBatchingStorageFn
	newBatchingStorageFn = func(
		child receiver.Storage,
		batchSize int,
		timeout time.Duration,
		opts ...receiver.BatchingOption,
	) *receiver.BatchingStorage {
		capturedOpts = opts
		return origFn(child, batchSize, timeout, opts...)
	}
	t.Cleanup(func() { newBatchingStorageFn = origFn })

	args := []string{"d8a-test"}
	setCurrentRunArgsForTest(t, args)

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// when
			_ = buildReceiverStorage(ctx, cmd, &noopPublisher{})
			return nil
		},
	}

	// then
	require.NoError(t, app.Run(context.Background(), args))
	assert.Empty(t, capturedOpts, "default wiring should inject no backend options")
}

func TestBuildReceiverStorage_FilesystemBackendInjectsFileBackend(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")
	unsetEnvForTest(t, "DELIVERY_MODE")
	unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")

	queueDir := t.TempDir()
	expectedDir := filepath.Join(queueDir, "receiver-batching")

	var calledFileFn bool
	var capturedCfg receiver.FileBatchingBackendConfig

	origFileFn := newFileBatchingBackendFn
	newFileBatchingBackendFn = func(cfg receiver.FileBatchingBackendConfig) receiver.BatchingBackend {
		calledFileFn = true
		capturedCfg = cfg
		return origFileFn(cfg)
	}
	t.Cleanup(func() { newFileBatchingBackendFn = origFileFn })

	args := []string{
		"d8a-test",
		"--receiver-batching-backend=filesystem",
		"--storage-queue-directory=" + queueDir,
	}
	setCurrentRunArgsForTest(t, args)

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// when
			_ = buildReceiverStorage(ctx, cmd, &noopPublisher{})
			return nil
		},
	}

	// then
	require.NoError(t, app.Run(context.Background(), args))
	assert.True(t, calledFileFn, "filesystem backend constructor should be called")
	assert.Equal(t, expectedDir, capturedCfg.Dir)
	assert.Equal(t, "pending_hits.json.gz", capturedCfg.FlushFileName)
}
