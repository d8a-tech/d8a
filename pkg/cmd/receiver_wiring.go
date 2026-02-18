package cmd

import (
	"context"
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

func buildReceiverStorage(ctx context.Context, cmd *cli.Command, publisher worker.Publisher) receiver.Storage {
	backend := strings.ToLower(cmd.String(queueBackendFlag.Name))

	if backend == "objectstorage" {
		backoffPublisher, err := publishers.NewBackoffPingingPublisher(
			ctx,
			publisher,
			pings.NewProcessHitsPingTask(encoding.GzipJSONEncoder),
			publishers.WithMinInterval(5*time.Second),
			publishers.WithIntervalExpFactor(1.5),
			publishers.WithMaxInterval(5*time.Minute),
		)
		if err == nil {
			publisher = backoffPublisher
		}
	} else {
		publisher = publishers.NewPingingPublisher(
			ctx,
			publisher,
			cmd.Duration(receiverBatchTimeoutFlag.Name),
			pings.NewProcessHitsPingTask(encoding.GzipJSONEncoder),
		)
	}

	publisher = worker.NewMonitoringPublisher(publisher)

	return receiver.NewBatchingStorage(
		storagepublisher.NewAdapter(encoding.GzipJSONEncoder, publisher),
		cmd.Int(receiverBatchSizeFlag.Name),
		cmd.Duration(receiverBatchTimeoutFlag.Name),
	)
}
