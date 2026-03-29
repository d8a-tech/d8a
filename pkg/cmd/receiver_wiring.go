package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/pings"
	"github.com/d8a-tech/d8a/pkg/publishers"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/storagepublisher"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/urfave/cli/v3"
)

func buildReceiverStorage(
	ctx context.Context,
	cmd *cli.Command,
	publisher worker.Publisher,
) (storage receiver.Storage, cleanup func(), err error) {
	backend := strings.ToLower(cmd.String(queueBackendFlag.Name))

	if backend == "objectstorage" {
		backoffPublisher, bErr := publishers.NewBackoffPingingPublisher(
			ctx,
			publisher,
			pings.NewProcessHitsPingTask(encoding.GzipJSONEncoder),
			publishers.WithMinInterval(5*time.Second),
			publishers.WithIntervalExpFactor(1.5),
			publishers.WithMaxInterval(5*time.Minute),
		)
		if bErr != nil {
			return nil, nil, fmt.Errorf("creating backoff pinging publisher: %w", bErr)
		}
		publisher = backoffPublisher
	} else {
		publisher = publishers.NewPingingPublisher(
			ctx,
			publisher,
			cmd.Duration(receiverBatchTimeoutFlag.Name),
			pings.NewProcessHitsPingTask(encoding.GzipJSONEncoder),
		)
	}

	publisher = worker.NewMonitoringPublisher(publisher)

	var opts []receiver.BatchingOption

	batchingBackend := strings.ToLower(cmd.String(receiverBatchingBackendFlag.Name))
	if batchingBackend == "filesystem" {
		opts = append(opts, receiver.WithBackend(
			receiver.NewFileBatchingBackend(receiver.FileBatchingBackendConfig{
				Dir: filepath.Join(
					cmd.String(storageQueueDirectoryFlag.Name),
					"receiver-batching",
				),
				FlushFileName: "pending_hits.json.gz",
			}),
		))
	}

	s, c := receiver.NewBatchingStorage(
		storagepublisher.NewAdapter(encoding.GzipJSONEncoder, publisher),
		cmd.Int(receiverBatchSizeFlag.Name),
		cmd.Duration(receiverBatchTimeoutFlag.Name),
		opts...,
	)
	return s, c, nil
}
