package e2e

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

// withMinIOContainer sets up a MinIO testcontainer and provides it to the callback function.
// It handles all setup and teardown automatically.
func withMinIOContainer(
	t *testing.T,
	callback func(bucket *blob.Bucket, container *minio.MinioContainer),
) {
	ctx := context.Background()

	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	require.NoError(t, err)

	defer func() {
		if err := testcontainers.TerminateContainer(minioContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	// Get connection details
	endpoint, err := minioContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// Create AWS config for S3 with MinIO
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
		awsconfig.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	// Create S3 client with MinIO endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
		o.UsePathStyle = true
	})

	// Create bucket first
	bucketName := "test-bucket"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	require.NoError(t, err)

	// Create bucket using Go CDK
	bucket, err := s3blob.OpenBucket(ctx, s3Client, bucketName, nil)
	require.NoError(t, err)

	callback(bucket, minioContainer)
}

// TestSeparateReceiverAndWorkerWithMinIO validates that hits flow from receiver through
// MinIO-backed objectstorage queue to worker when running as separate processes
func TestSeparateReceiverAndWorkerWithMinIO(t *testing.T) {
	const port = 17033

	// given - setup
	withMinIOContainer(t, func(_ *blob.Bucket, container *minio.MinioContainer) {
		// Build binary once
		binaryPath := buildBinary(t)
		t.Cleanup(func() { _ = os.Remove(binaryPath) })

		// Get connection string from MinIO container
		ctx := context.Background()
		connectionString, err := container.ConnectionString(ctx)
		require.NoError(t, err)

		// Parse connection string to extract host and port
		// Connection string format: "host:port"
		parts := strings.Split(connectionString, ":")
		require.Len(t, parts, 2, "connection string should be host:port")
		minioHost := parts[0]
		minioPort, err := strconv.Atoi(parts[1])
		require.NoError(t, err)

		// Create shared temp storage directory (for BoltDB)
		_, storageDir := buildSharedTempDirectories(t)

		// Generate configs for receiver and worker with objectstorage queue
		receiverConfigPath := newTestConfigBuilder().
			WithPort(port).
			WithStorageDirectory(storageDir).
			WithWarehouse("noop").
			WithSessionTimeout(2 * time.Second).
			WithQueueBackend("objectstorage").
			WithObjectStorageType("s3").
			WithObjectStorageS3Host(minioHost).
			WithObjectStorageS3Port(minioPort).
			WithObjectStorageS3AccessKey("minioadmin").
			WithObjectStorageS3SecretKey("minioadmin").
			WithObjectStorageS3Bucket("test-bucket").
			WithObjectStorageS3CreateBucket(true).
			WithQueueObjectStorageMinInterval(10 * time.Millisecond).
			WithQueueObjectStorageMaxInterval(50 * time.Millisecond).
			WithQueueObjectStorageIntervalExpFactor(1.1).
			Build(t)

		workerConfigPath := newTestConfigBuilder().
			WithStorageDirectory(storageDir).
			WithWarehouse("noop").
			WithSessionTimeout(2 * time.Second).
			WithQueueBackend("objectstorage").
			WithObjectStorageType("s3").
			WithObjectStorageS3Host(minioHost).
			WithObjectStorageS3Port(minioPort).
			WithObjectStorageS3AccessKey("minioadmin").
			WithObjectStorageS3SecretKey("minioadmin").
			WithObjectStorageS3Bucket("test-bucket").
			WithObjectStorageS3CreateBucket(true).
			WithQueueObjectStorageMinInterval(10 * time.Millisecond).
			WithQueueObjectStorageMaxInterval(50 * time.Millisecond).
			WithQueueObjectStorageIntervalExpFactor(1.1).
			Build(t)

		// when - execution
		// Start worker process in background
		workerHandle, err := startProcessInBackground(t, binaryPath, "worker", "--config", workerConfigPath)
		require.NoError(t, err, "failed to start worker process")

		// Start receiver process in background
		receiverHandle, err := startProcessInBackground(t, binaryPath, "receiver", "--config", receiverConfigPath)
		require.NoError(t, err, "failed to start receiver process")

		// Wait for receiver to be ready (healthz endpoint)
		require.True(t, waitForServerReady(port, 10*time.Second), "receiver should become ready")

		// Send GA4 tracking request sequence (3 hits from existing test)
		striker := NewGA4RequestGenerator("localhost", port)
		err = striker.Replay([]HitSequenceItem{
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
		require.NoError(t, err, "failed to replay GA4 sequence")

		// then - verification
		// Verify receiver started successfully
		assert.True(
			t,
			receiverHandle.logs.waitFor("Starting server", 5*time.Second),
			"receiver should start successfully",
		)

		// Verify worker logs show "Appending to proto-sessions" messages (worker processes hits)
		assert.True(
			t,
			workerHandle.logs.waitFor("Appending to proto-sessions", 5*time.Second),
			"worker should process hits from queue",
		)

		// Verify worker logs show "flushing batch" to warehouse
		assert.True(
			t,
			workerHandle.logs.waitFor("flushing batch", 10*time.Second),
			"worker should flush batch to warehouse",
		)

		// Verify no error logs from either process
		receiverLines := receiverHandle.logs.GetLines()
		for _, line := range receiverLines {
			assert.NotContains(t, line, "level=error", "receiver should not log errors")
			assert.NotContains(t, line, "level=fatal", "receiver should not log fatal errors")
		}

		workerLines := workerHandle.logs.GetLines()
		for _, line := range workerLines {
			assert.NotContains(t, line, "level=error", "worker should not log errors")
			assert.NotContains(t, line, "level=fatal", "worker should not log fatal errors")
		}

		// Both processes shut down cleanly (cleanup happens automatically via t.Cleanup)
		fmt.Println("Test completed successfully with MinIO backend")
	})
}
