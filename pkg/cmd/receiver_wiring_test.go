package cmd

import (
	"compress/gzip"
	"context"
	"encoding/json"
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
			storage := buildReceiverStorage(ctx, cmd, &noopPublisher{})
			if closableStorage, ok := storage.(interface{ Close() }); ok {
				t.Cleanup(closableStorage.Close)
			}

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
			storage := buildReceiverStorage(ctx, cmd, &noopPublisher{})
			if closableStorage, ok := storage.(interface{ Close() }); ok {
				t.Cleanup(closableStorage.Close)
			}

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

	flushFilePath := filepath.Join(expectedDir, "pending_hits.json.gz")
	file, err := os.Open(flushFilePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	gz, err := gzip.NewReader(file)
	require.NoError(t, err)
	t.Cleanup(func() { _ = gz.Close() })

	var hitsFromFile []*hits.Hit
	err = json.NewDecoder(gz).Decode(&hitsFromFile)
	require.NoError(t, err)
	require.Len(t, hitsFromFile, 1)
	assert.Equal(t, "property-a", hitsFromFile[0].PropertyID)
}
