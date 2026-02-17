package objectstorage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

// mockTaskHandler is a test handler that captures processed tasks
type mockTaskHandler struct {
	processedTasks []*worker.Task
	shouldError    bool
	errorCount     int
	currentCall    int
}

func (m *mockTaskHandler) Handle(task *worker.Task) error {
	m.currentCall++
	if m.shouldError && m.currentCall <= m.errorCount {
		return fmt.Errorf("mock error for task %s", task.ID())
	}
	m.processedTasks = append(m.processedTasks, task)
	return nil
}

func TestNewConsumer(t *testing.T) {
	tests := []struct {
		name          string
		ctx           context.Context
		bucket        *blob.Bucket
		messageFormat worker.MessageFormat
		opts          []Option
		expectErr     bool
		expectedErr   string
	}{
		{
			name:          "valid consumer creation",
			ctx:           context.Background(),
			bucket:        memblob.OpenBucket(nil),
			messageFormat: worker.NewBinaryMessageFormat(),
			opts:          nil,
			expectErr:     false,
		},
		{
			name:          "nil bucket",
			ctx:           context.Background(),
			bucket:        nil,
			messageFormat: worker.NewBinaryMessageFormat(),
			opts:          nil,
			expectErr:     true,
			expectedErr:   "bucket cannot be nil",
		},
		{
			name:          "nil message format",
			ctx:           context.Background(),
			bucket:        memblob.OpenBucket(nil),
			messageFormat: nil,
			opts:          nil,
			expectErr:     true,
			expectedErr:   "messageFormat cannot be nil",
		},
		{
			name:          "no options uses default",
			ctx:           context.Background(),
			bucket:        memblob.OpenBucket(nil),
			messageFormat: worker.NewBinaryMessageFormat(),
			opts:          nil,
			expectErr:     false,
		},
		{
			name:          "invalid config",
			ctx:           context.Background(),
			bucket:        memblob.OpenBucket(nil),
			messageFormat: worker.NewBinaryMessageFormat(),
			opts: []Option{
				WithMaxItemsToReadAtOnce(-1), // Invalid value
			},
			expectErr:   true,
			expectedErr: "invalid config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			consumer, err := NewConsumer(tt.ctx, tt.bucket, tt.messageFormat, tt.opts...)

			// then
			if tt.expectErr {
				assert.Error(t, err)
				if tt.expectedErr != "" {
					assert.Contains(t, err.Error(), tt.expectedErr)
				}
				assert.Nil(t, consumer)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, consumer)
				assert.NotNil(t, consumer.config)
			}
		})
	}
}

func TestConsumer_processNextBatch_EmptyBucket(t *testing.T) {
	// given
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	messageFormat := worker.NewBinaryMessageFormat()

	consumer, err := NewConsumer(ctx, bucket, messageFormat, WithMinInterval(time.Millisecond))
	require.NoError(t, err)

	handler := &mockTaskHandler{}

	// when
	_, err = consumer.processNextBatch(handler.Handle)

	// then
	assert.NoError(t, err)
	assert.Empty(t, handler.processedTasks)
}

func TestConsumer_processNextBatch_SingleTask(t *testing.T) {
	// given
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	messageFormat := worker.NewBinaryMessageFormat()

	// Create and publish a task first
	publisher, err := NewPublisher(bucket, messageFormat)
	require.NoError(t, err)

	task := worker.NewTask("test-task", map[string]string{"key": "value"}, []byte("test data"))
	err = publisher.Publish(task)
	require.NoError(t, err)

	// Create consumer
	consumer, err := NewConsumer(ctx, bucket, messageFormat)
	require.NoError(t, err)

	handler := &mockTaskHandler{}

	// when
	_, err = consumer.processNextBatch(handler.Handle)

	// then
	assert.NoError(t, err)
	assert.Len(t, handler.processedTasks, 1)

	processedTask := handler.processedTasks[0]
	assert.Equal(t, task.Type, processedTask.Type)
	assert.Equal(t, task.Headers, processedTask.Headers)
	assert.Equal(t, task.Body, processedTask.Body)

	// Verify task was cleaned up (deleted)
	iter := bucket.List(&blob.ListOptions{Prefix: ""})
	_, err = iter.Next(ctx)
	assert.Error(t, err) // Should be no more items
}

func TestConsumer_processNextBatch_MultipleTasks_FIFO(t *testing.T) {
	// given
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	messageFormat := worker.NewBinaryMessageFormat()

	publisher, err := NewPublisher(bucket, messageFormat)
	require.NoError(t, err)

	// Publish tasks with small delays to ensure timestamp ordering
	tasks := []*worker.Task{
		worker.NewTask("task-1", map[string]string{"order": "1"}, []byte("first")),
		worker.NewTask("task-2", map[string]string{"order": "2"}, []byte("second")),
		worker.NewTask("task-3", map[string]string{"order": "3"}, []byte("third")),
	}

	for i, task := range tasks {
		err = publisher.Publish(task)
		require.NoError(t, err)

		// Small delay to ensure timestamp ordering
		if i < len(tasks)-1 {
			time.Sleep(time.Millisecond)
		}
	}

	consumer, err := NewConsumer(ctx, bucket, messageFormat)
	require.NoError(t, err)

	handler := &mockTaskHandler{}

	// when
	_, err = consumer.processNextBatch(handler.Handle)

	// then
	assert.NoError(t, err)
	assert.Len(t, handler.processedTasks, 3)

	// Verify FIFO order by checking headers
	assert.Equal(t, "1", handler.processedTasks[0].Headers["order"])
	assert.Equal(t, "2", handler.processedTasks[1].Headers["order"])
	assert.Equal(t, "3", handler.processedTasks[2].Headers["order"])
}

func TestConsumer_processNextBatch_HandlerError(t *testing.T) {
	// given
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	messageFormat := worker.NewBinaryMessageFormat()

	publisher, err := NewPublisher(bucket, messageFormat)
	require.NoError(t, err)

	task := worker.NewTask("test-task", map[string]string{"key": "value"}, []byte("test data"))
	err = publisher.Publish(task)
	require.NoError(t, err)

	consumer, err := NewConsumer(ctx, bucket, messageFormat)
	require.NoError(t, err)

	handler := &mockTaskHandler{shouldError: true, errorCount: 1}

	// when
	_, err = consumer.processNextBatch(handler.Handle)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "errors processing tasks")
}

func TestConsumer_processNextBatch_MaxItemsLimit(t *testing.T) {
	// given
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	messageFormat := worker.NewBinaryMessageFormat()

	publisher, err := NewPublisher(bucket, messageFormat, WithMaxItemsToReadAtOnce(2))
	require.NoError(t, err)

	// Publish 5 tasks
	for i := 0; i < 5; i++ {
		task := worker.NewTask(
			fmt.Sprintf("task-%d", i),
			map[string]string{"order": fmt.Sprintf("%d", i)},
			[]byte(fmt.Sprintf("data-%d", i)),
		)
		err = publisher.Publish(task)
		require.NoError(t, err)
		time.Sleep(time.Millisecond) // Ensure timestamp ordering
	}

	consumer, err := NewConsumer(ctx, bucket, messageFormat, WithMaxItemsToReadAtOnce(2))
	require.NoError(t, err)

	handler := &mockTaskHandler{}

	// when - first batch
	_, err = consumer.processNextBatch(handler.Handle)

	// then
	assert.NoError(t, err)
	assert.Len(t, handler.processedTasks, 2) // Should process only 2 due to limit

	// Verify remaining tasks are still in bucket
	iter := bucket.List(&blob.ListOptions{Prefix: ""})
	remainingCount := 0
	for {
		obj, err := iter.Next(ctx)
		if err != nil {
			break
		}
		if obj != nil {
			remainingCount++
		}
	}
	assert.Equal(t, 3, remainingCount) // 3 tasks should remain
}

func TestConsumer_processNextBatch_WithPrefix(t *testing.T) {
	// given
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	messageFormat := worker.NewBinaryMessageFormat()

	publisher, err := NewPublisher(bucket, messageFormat, WithPrefix("prefix-a"))
	require.NoError(t, err)

	task := worker.NewTask("test-task", map[string]string{"partition": "123"}, []byte("partitioned data"))
	err = publisher.Publish(task)
	require.NoError(t, err)

	otherPublisher, err := NewPublisher(bucket, messageFormat, WithPrefix("prefix-b"))
	require.NoError(t, err)
	err = otherPublisher.Publish(worker.NewTask("other-task", map[string]string{"partition": "999"}, []byte("other")))
	require.NoError(t, err)

	consumer, err := NewConsumer(ctx, bucket, messageFormat, WithPrefix("prefix-a"))
	require.NoError(t, err)

	handler := &mockTaskHandler{}

	// when
	_, err = consumer.processNextBatch(handler.Handle)

	// then
	assert.NoError(t, err)
	assert.Len(t, handler.processedTasks, 1)

	processedTask := handler.processedTasks[0]
	assert.Equal(t, "123", processedTask.Headers["partition"])
}

// Helper function to create a bucket with invalid objects for error testing
func createBucketWithInvalidTask(t *testing.T) *blob.Bucket {
	bucket := memblob.OpenBucket(nil)
	ctx := context.Background()

	// Write invalid data that can't be deserialized
	invalidData := []byte("this is not valid task data")
	key := GenerateTaskKey(GenerateTaskID())

	w, err := bucket.NewWriter(ctx, key, nil)
	require.NoError(t, err)

	_, err = w.Write(invalidData)
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)

	return bucket
}

func TestConsumer_processNextBatch_DeserializationError(t *testing.T) {
	// given
	ctx := context.Background()
	bucket := createBucketWithInvalidTask(t)
	messageFormat := worker.NewBinaryMessageFormat()

	consumer, err := NewConsumer(ctx, bucket, messageFormat)
	require.NoError(t, err)

	handler := &mockTaskHandler{}

	// when
	_, err = consumer.processNextBatch(handler.Handle)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to deserialize task")
	assert.Empty(t, handler.processedTasks)
}
