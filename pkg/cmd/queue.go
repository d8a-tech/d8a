package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/d8a-tech/d8a/pkg/objectstorage"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/urfave/cli/v3"
	"gocloud.dev/blob"
)

const queueBackendFilesystem = "filesystem"

type Queue struct {
	Publisher worker.Publisher
	Consumer  worker.Consumer
	Cleanup   func() error
}

func buildQueue(ctx context.Context, cmd *cli.Command) (*Queue, error) {
	backend := strings.ToLower(cmd.String(queueBackendFlag.Name))
	switch backend {
	case "", queueBackendFilesystem:
		return buildFilesystemQueue(ctx, cmd.String(storageQueueDirectoryFlag.Name))
	case "objectstorage":
		bucket, cleanup, err := createBucket(ctx, cmd)
		if err != nil {
			return nil, err
		}
		q, err := buildObjectStorageQueue(
			ctx,
			bucket,
			cmd.String(queueObjectPrefixFlag.Name),
			cmd.Duration(queueObjectStorageMinIntervalFlag.Name),
			cmd.Duration(queueObjectStorageMaxIntervalFlag.Name),
			cmd.Float64(queueObjectStorageIntervalExpFactorFlag.Name),
			cmd.Int(queueObjectStorageMaxItemsToReadAtOnceFlag.Name),
		)
		if err != nil {
			_ = cleanup()
			return nil, err
		}
		q.Cleanup = cleanup
		return q, nil
	default:
		return nil, fmt.Errorf("unsupported queue backend: %s", backend)
	}
}

func buildFilesystemQueue(ctx context.Context, dir string) (*Queue, error) {
	pub, err := worker.NewFilesystemDirectoryPublisher(
		dir,
		worker.NewBinaryMessageFormat(),
	)
	if err != nil {
		return nil, fmt.Errorf("create filesystem queue publisher: %w", err)
	}
	con, err := worker.NewFilesystemDirectoryConsumer(
		ctx,
		dir,
		worker.NewBinaryMessageFormat(),
	)
	if err != nil {
		return nil, fmt.Errorf("create filesystem queue consumer: %w", err)
	}
	return &Queue{Publisher: pub, Consumer: con, Cleanup: func() error { return nil }}, nil
}

func buildObjectStorageQueue(
	ctx context.Context,
	bucket *blob.Bucket,
	prefix string,
	minInterval time.Duration,
	maxInterval time.Duration,
	intervalExpFactor float64,
	maxItemsToReadAtOnce int,
) (*Queue, error) {
	var opts []objectstorage.Option
	if prefix != "" {
		opts = append(opts, objectstorage.WithPrefix(prefix))
	}
	opts = append(opts,
		objectstorage.WithMinInterval(minInterval),
		objectstorage.WithMaxInterval(maxInterval),
		objectstorage.WithIntervalExpFactor(intervalExpFactor),
		objectstorage.WithMaxItemsToReadAtOnce(maxItemsToReadAtOnce),
	)

	pub, err := objectstorage.NewPublisher(bucket, worker.NewBinaryMessageFormat(), opts...)
	if err != nil {
		return nil, fmt.Errorf("create objectstorage queue publisher: %w", err)
	}
	con, err := objectstorage.NewConsumer(ctx, bucket, worker.NewBinaryMessageFormat(), opts...)
	if err != nil {
		return nil, fmt.Errorf("create objectstorage queue consumer: %w", err)
	}
	return &Queue{Publisher: pub, Consumer: con, Cleanup: func() error { return nil }}, nil
}
