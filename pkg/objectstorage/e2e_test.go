package objectstorage

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func TestPublisher_Integration_MinIO_BasicPublish(t *testing.T) {
	withMinIOContainer(t, func(bucket *blob.Bucket, _ *minio.MinioContainer) {
		// given
		messageFormat := worker.NewBinaryMessageFormat()

		publisher, err := NewPublisher(bucket, messageFormat)
		require.NoError(t, err)

		task := worker.NewTask("integration-test", map[string]string{
			"test-header": "test-value",
			"priority":    "high",
		}, []byte("MinIO integration test data"))

		// when
		err = publisher.Publish(task)

		// then
		assert.NoError(t, err)

		// Verify the object was stored in MinIO by listing with the correct prefix
		ctx := context.Background()
		iter := bucket.List(&blob.ListOptions{
			Prefix: "",
		})
		obj, err := iter.Next(ctx)
		require.NoError(t, err, "Should find exactly one object with prefix %s", "")
		require.NotNil(t, obj, "Should find object in the bucket")

		// Verify we can read back the data
		data, err := bucket.ReadAll(ctx, obj.Key)
		require.NoError(t, err)

		// Verify we can deserialize the data
		deserializedTask, err := messageFormat.Deserialize(data)
		require.NoError(t, err)

		assert.Equal(t, task.Type, deserializedTask.Type)
		assert.Equal(t, task.Headers, deserializedTask.Headers)
		assert.Equal(t, task.Body, deserializedTask.Body)
	})
}

func TestPublisher_Integration_MinIO_MultiplePublishes(t *testing.T) {
	withMinIOContainer(t, func(bucket *blob.Bucket, _ *minio.MinioContainer) {
		// given
		messageFormat := worker.NewBinaryMessageFormat()

		publisher, err := NewPublisher(bucket, messageFormat)
		require.NoError(t, err)

		tasks := []*worker.Task{
			worker.NewTask("task-1", map[string]string{"batch": "1"}, []byte("task 1 data")),
			worker.NewTask("task-2", map[string]string{"batch": "1"}, []byte("task 2 data")),
			worker.NewTask("task-3", map[string]string{"batch": "1"}, []byte("task 3 data")),
		}

		// when - publish all tasks
		for _, task := range tasks {
			err = publisher.Publish(task)
			require.NoError(t, err)
		}

		// then - verify all tasks were stored
		ctx := context.Background()
		iter := bucket.List(&blob.ListOptions{
			Prefix: "",
		})

		var storedObjects []string
		for {
			obj, err := iter.Next(ctx)
			if err != nil {
				break
			}
			if obj != nil {
				storedObjects = append(storedObjects, obj.Key)
			}
		}

		// Verify we have all tasks
		assert.Len(t, storedObjects, 3)

		// Verify each task ID is present in the keys
		for _, task := range tasks {
			found := false
			for _, key := range storedObjects {
				if strings.Contains(key, task.ID()) {
					found = true
					break
				}
			}
			assert.True(t, found, "Task %s not found in stored objects", task.ID())
		}

		// Verify keys are properly ordered (lexicographic timestamp ordering)
		if len(storedObjects) >= 2 {
			// Keys should be lexicographically ordered due to timestamp prefix
			assert.True(t, storedObjects[0] < storedObjects[1], "Keys should be ordered")
			if len(storedObjects) >= 3 {
				assert.True(t, storedObjects[1] < storedObjects[2], "Keys should be ordered")
			}
		}
	})
}

func TestPublisher_Integration_MinIO_WithRetries(t *testing.T) {
	withMinIOContainer(t, func(bucket *blob.Bucket, _ *minio.MinioContainer) {
		// given
		messageFormat := worker.NewBinaryMessageFormat()

		publisher, err := NewPublisher(bucket, messageFormat, WithRetryAttempts(3))
		require.NoError(t, err)

		task := worker.NewTask("retry-test", map[string]string{
			"test-type": "retry",
		}, []byte("retry test data"))

		// when
		err = publisher.PublishWithRetry(task)

		// then
		assert.NoError(t, err)

		// Verify the task was published successfully
		ctx := context.Background()
		iter := bucket.List(&blob.ListOptions{
			Prefix: "",
		})

		obj, err := iter.Next(ctx)
		require.NoError(t, err, "Should find exactly one object with prefix %s", "")
		require.NotNil(t, obj)
	})
}

func TestPublisher_Integration_MinIO_EmptyPrefix(t *testing.T) {
	withMinIOContainer(t, func(bucket *blob.Bucket, _ *minio.MinioContainer) {
		// given
		messageFormat := worker.NewBinaryMessageFormat()

		publisher, err := NewPublisher(bucket, messageFormat)
		require.NoError(t, err)

		task := worker.NewTask("no-prefix-test", map[string]string{
			"prefix": "none",
		}, []byte("no prefix test data"))

		// when
		err = publisher.Publish(task)

		// then
		assert.NoError(t, err)

		// Verify the object was stored with correct key format
		ctx := context.Background()
		iter := bucket.List(&blob.ListOptions{
			Prefix: "",
		})

		obj, err := iter.Next(ctx)
		require.NoError(t, err, "Should find exactly one object with prefix %s", "")
		require.NotNil(t, obj)
	})
}

func TestConsumer_Integration_MinIO_BasicConsume(t *testing.T) {
	withMinIOContainer(t, func(bucket *blob.Bucket, _ *minio.MinioContainer) {
		// given
		messageFormat := worker.NewBinaryMessageFormat()

		// Publish some tasks first
		publisher, err := NewPublisher(bucket, messageFormat, WithMinInterval(time.Millisecond*10))
		require.NoError(t, err)

		tasks := []*worker.Task{
			worker.NewTask("task-1", map[string]string{"order": "1"}, []byte("first task")),
			worker.NewTask("task-2", map[string]string{"order": "2"}, []byte("second task")),
		}

		for _, task := range tasks {
			err = publisher.Publish(task)
			require.NoError(t, err)
		}

		// Create consumer
		ctx := context.Background()
		consumer, err := NewConsumer(ctx, bucket, messageFormat, WithMinInterval(time.Millisecond*10))
		require.NoError(t, err)

		// Track processed tasks
		var processedTasks []*worker.Task
		handler := func(task *worker.Task) error {
			processedTasks = append(processedTasks, task)
			return nil
		}

		// when
		_, err = consumer.processNextBatch(handler)

		// then
		assert.NoError(t, err)
		assert.Len(t, processedTasks, 2)

		// Verify tasks were processed in order
		assert.Equal(t, "1", processedTasks[0].Headers["order"])
		assert.Equal(t, "2", processedTasks[1].Headers["order"])

		// Verify cleanup (delete strategy) - no more tasks should remain
		iter := bucket.List(&blob.ListOptions{Prefix: ""})
		_, err = iter.Next(ctx)
		assert.Error(t, err) // Should be EOF - no more items
	})
}

func TestPublisherConsumer_Integration_MinIO_FullFlow(t *testing.T) {
	withMinIOContainer(t, func(bucket *blob.Bucket, _ *minio.MinioContainer) {
		// given
		messageFormat := worker.NewBinaryMessageFormat()

		publisher, err := NewPublisher(bucket, messageFormat, WithMaxItemsToReadAtOnce(3))
		require.NoError(t, err)

		ctx := context.Background()
		consumer, err := NewConsumer(ctx, bucket, messageFormat, WithMaxItemsToReadAtOnce(3))
		require.NoError(t, err)

		// Publish multiple batches of tasks
		totalTasks := 7
		tasks := make([]*worker.Task, totalTasks)
		for i := 0; i < totalTasks; i++ {
			tasks[i] = worker.NewTask(
				fmt.Sprintf("task-%d", i),
				map[string]string{"batch": "full-flow", "index": fmt.Sprintf("%d", i)},
				[]byte(fmt.Sprintf("data for task %d", i)),
			)
			err = publisher.Publish(tasks[i])
			require.NoError(t, err)
		}

		var allProcessedTasks []*worker.Task
		handler := func(task *worker.Task) error {
			allProcessedTasks = append(allProcessedTasks, task)
			return nil
		}

		// when - process first batch (should be limited to 3 items)
		_, err = consumer.processNextBatch(handler)
		assert.NoError(t, err)
		assert.Len(t, allProcessedTasks, 3) // Limited by MaxItemsToReadAtOnce

		// when - process second batch
		_, err = consumer.processNextBatch(handler)
		assert.NoError(t, err)
		assert.Len(t, allProcessedTasks, 6) // 3 + 3 more

		// when - process remaining tasks
		_, err = consumer.processNextBatch(handler)
		assert.NoError(t, err)
		assert.Len(t, allProcessedTasks, 7) // All tasks processed

		// when - try to process again (should be no more tasks)
		beforeLen := len(allProcessedTasks)
		_, err = consumer.processNextBatch(handler)
		assert.NoError(t, err)
		assert.Len(t, allProcessedTasks, beforeLen) // No new tasks processed

		// then - verify all tasks were processed with correct data
		for i, processedTask := range allProcessedTasks {
			assert.Equal(t, "full-flow", processedTask.Headers["batch"])
			assert.Equal(t, fmt.Sprintf("%d", i), processedTask.Headers["index"])
			assert.Equal(t, []byte(fmt.Sprintf("data for task %d", i)), processedTask.Body)
		}

		// Verify all tasks were cleaned up
		iter := bucket.List(&blob.ListOptions{Prefix: ""})
		_, err = iter.Next(ctx)
		assert.Error(t, err) // Should be EOF - no more items
	})
}

func TestConsumer_Integration_MinIO_ErrorHandling(t *testing.T) {
	withMinIOContainer(t, func(bucket *blob.Bucket, _ *minio.MinioContainer) {
		// given
		messageFormat := worker.NewBinaryMessageFormat()

		publisher, err := NewPublisher(bucket, messageFormat)
		require.NoError(t, err)

		task := worker.NewTask("error-task", map[string]string{"type": "error"}, []byte("error test"))
		err = publisher.Publish(task)
		require.NoError(t, err)

		ctx := context.Background()
		consumer, err := NewConsumer(ctx, bucket, messageFormat)
		require.NoError(t, err)

		errorCount := 0
		handler := func(task *worker.Task) error {
			errorCount++
			return fmt.Errorf("simulated processing error for task %s", task.ID())
		}

		// when
		_, err = consumer.processNextBatch(handler)

		// then
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "errors processing tasks")
		assert.Equal(t, 1, errorCount)

		// Task should NOT be cleaned up on error
		iter := bucket.List(&blob.ListOptions{Prefix: ""})
		obj, err := iter.Next(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, obj) // Task should still be there
	})
}
