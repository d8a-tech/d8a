package cmd

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/urfave/cli/v3"
)

func TestReceiverWorkerQueue_ObjectStorage_MinIO(t *testing.T) {
	ctx := context.Background()

	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(minioContainer)
	})

	endpoint, err := minioContainer.ConnectionString(ctx)
	require.NoError(t, err)
	host, portStr, err := net.SplitHostPort(endpoint)
	require.NoError(t, err)

	var consumed []byte

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: getServerFlags(),
		Action: func(_ context.Context, c *cli.Command) error {
			q, err := buildQueue(ctx, c)
			if err != nil {
				return err
			}
			defer func() { _ = q.Cleanup() }()

			publishTask := &worker.Task{Type: "t", Headers: map[string]string{"x": "y"}, Body: []byte("hello")}
			if err := q.Publisher.Publish(publishTask); err != nil {
				return fmt.Errorf("publish: %w", err)
			}

			consumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- q.Consumer.Consume(func(task *worker.Task) error {
					consumed = append([]byte(nil), task.Body...)
					cancel()
					return nil
				})
			}()

			<-consumeCtx.Done()
			select {
			case err := <-errCh:
				return err
			default:
				return nil
			}
		},
	}

	args := []string{
		"d8a-test",
		"--queue-backend=objectstorage",
		"--queue-object-prefix=it/prefix",
		"--object-storage-type=s3",
		"--object-storage-s3-host=" + host,
		"--object-storage-s3-port=" + portStr,
		"--object-storage-s3-bucket=d8a-it",
		"--object-storage-s3-access-key=minioadmin",
		"--object-storage-s3-secret-key=minioadmin",
		"--object-storage-s3-region=us-east-1",
		"--object-storage-s3-protocol=http",
		"--object-storage-s3-create-bucket=true",
	}

	require.NoError(t, app.Run(ctx, args))
	require.Equal(t, []byte("hello"), consumed)
}
