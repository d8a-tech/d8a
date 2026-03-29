package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/d8a-tech/d8a/pkg/hits"
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

	queueDir := t.TempDir()
	expectedDir := filepath.Join(queueDir, "receiver-batching")

	args := []string{"d8a-test", "--storage-queue-directory=" + queueDir}
	setCurrentRunArgsForTest(t, args)

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// when
			storage, cleanup := buildReceiverStorage(ctx, cmd, &noopPublisher{})
			defer cleanup()

			err := storage.Push([]*hits.Hit{{PropertyID: "property-a"}})
			require.NoError(t, err)

			_, err = os.Stat(expectedDir)
			assert.True(t, os.IsNotExist(err), "memory backend should not create filesystem batching directory")
			return nil
		},
	}

	// then
	require.NoError(t, app.Run(context.Background(), args))
}

func TestBuildReceiverStorage_FilesystemBackendInjectsFileBackend(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")
	unsetEnvForTest(t, "DELIVERY_MODE")
	unsetEnvForTest(t, "RECEIVER_BATCHING_BACKEND")

	queueDir := t.TempDir()
	expectedDir := filepath.Join(queueDir, "receiver-batching")

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
			storage, cleanup := buildReceiverStorage(ctx, cmd, &noopPublisher{})
			defer cleanup()

			err := storage.Push([]*hits.Hit{{PropertyID: "property-a"}})
			require.NoError(t, err)

			flushFilePath := filepath.Join(expectedDir, "pending_hits.json.gz")
			_, err = os.Stat(flushFilePath)
			require.NoError(t, err)
			return nil
		},
	}

	// then
	require.NoError(t, app.Run(context.Background(), args))
}
