package objectstorage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

// mockMessageFormat implements worker.MessageFormat for testing
type mockMessageFormat struct {
	serializeFunc   func(*worker.Task) ([]byte, error)
	deserializeFunc func([]byte) (*worker.Task, error)
}

func (m *mockMessageFormat) Serialize(t *worker.Task) ([]byte, error) {
	if m.serializeFunc != nil {
		return m.serializeFunc(t)
	}
	return []byte("serialized-data"), nil
}

func (m *mockMessageFormat) Deserialize(data []byte) (*worker.Task, error) {
	if m.deserializeFunc != nil {
		return m.deserializeFunc(data)
	}
	return &worker.Task{}, nil
}

func TestNewPublisher(t *testing.T) {
	tests := []struct {
		name           string
		bucket         *blob.Bucket
		messageFormat  worker.MessageFormat
		opts           []Option
		expectError    bool
		expectedErrMsg string
	}{
		{
			name:          "valid parameters with default config",
			bucket:        memblob.OpenBucket(nil),
			messageFormat: &mockMessageFormat{},
			opts:          nil,
			expectError:   false,
		},
		{
			name:          "valid parameters with custom config",
			bucket:        memblob.OpenBucket(nil),
			messageFormat: &mockMessageFormat{},
			opts: []Option{
				WithRetryAttempts(3),
				WithMaxItemsToReadAtOnce(50),
				WithMinInterval(time.Millisecond * 10),
				WithIntervalExpFactor(1.1),
				WithMaxInterval(time.Minute * 5),
				WithProcessingTimeout(time.Minute * 5),
			},
			expectError: false,
		},
		{
			name:           "nil bucket",
			bucket:         nil,
			messageFormat:  &mockMessageFormat{},
			opts:           nil,
			expectError:    true,
			expectedErrMsg: "bucket cannot be nil",
		},
		{
			name:           "nil message format",
			bucket:         memblob.OpenBucket(nil),
			messageFormat:  nil,
			opts:           nil,
			expectError:    true,
			expectedErrMsg: "messageFormat cannot be nil",
		},
		{
			name:          "invalid config",
			bucket:        memblob.OpenBucket(nil),
			messageFormat: &mockMessageFormat{},
			opts: []Option{
				WithMaxItemsToReadAtOnce(-1),
			},
			expectError:    true,
			expectedErrMsg: "invalid config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			publisher, err := NewPublisher(tt.bucket, tt.messageFormat, tt.opts...)

			// then
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErrMsg)
				assert.Nil(t, publisher)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, publisher)
				assert.NotNil(t, publisher.bucket)
				assert.NotNil(t, publisher.messageFormat)
				assert.NotNil(t, publisher.config)
			}
		})
	}
}

func TestPublisher_Publish(t *testing.T) {
	tests := []struct {
		name                 string
		task                 *worker.Task
		serializeFunc        func(*worker.Task) ([]byte, error)
		opts                 []Option
		expectError          bool
		expectedErrMsg       string
		expectObjectInBucket bool
	}{
		{
			name: "successful publish",
			task: &worker.Task{
				Type:    "test-task",
				Headers: map[string]string{"task_id": "test-123"},
				Body:    []byte("test-data"),
			},
			serializeFunc:        nil, // Use default mock behavior
			opts:                 nil,
			expectError:          false,
			expectObjectInBucket: true,
		},
		{
			name: "successful publish with generated task ID",
			task: &worker.Task{
				Type:    "test-task",
				Headers: map[string]string{},
				Body:    []byte("test-data"),
			},
			serializeFunc:        nil,
			opts:                 nil,
			expectError:          false,
			expectObjectInBucket: true,
		},
		{
			name:                 "nil task",
			task:                 nil,
			opts:                 nil,
			expectError:          true,
			expectedErrMsg:       "task cannot be nil",
			expectObjectInBucket: false,
		},
		{
			name: "serialization error",
			task: &worker.Task{
				Type:    "test-task",
				Headers: map[string]string{"task_id": "test-123"},
				Body:    []byte("test-data"),
			},
			serializeFunc: func(*worker.Task) ([]byte, error) {
				return nil, errors.New("serialization failed")
			},
			opts:                 nil,
			expectError:          true,
			expectedErrMsg:       "failed to serialize task",
			expectObjectInBucket: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			bucket := memblob.OpenBucket(nil)
			messageFormat := &mockMessageFormat{
				serializeFunc: tt.serializeFunc,
			}

			publisher, err := NewPublisher(bucket, messageFormat, tt.opts...)
			require.NoError(t, err)

			// when
			err = publisher.Publish(tt.task)

			// then
			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrMsg)
				}
			} else {
				assert.NoError(t, err)
			}

			// Check if object was created in bucket
			if tt.expectObjectInBucket {
				ctx := context.Background()
				iter := bucket.List(&blob.ListOptions{})
				hasObject := false
				for {
					obj, err := iter.Next(ctx)
					if err != nil {
						break
					}
					if obj != nil {
						hasObject = true
						break
					}
				}
				assert.True(t, hasObject, "Expected object to be created in bucket")
			}
		})
	}
}

func TestPublisher_PublishWithRetry(t *testing.T) {
	tests := []struct {
		name               string
		task               *worker.Task
		retryAttempts      int
		serialFailureCount int // How many times serialization should fail before succeeding
		expectError        bool
		expectedErrMsg     string
	}{
		{
			name: "succeed on first attempt",
			task: &worker.Task{
				Type:    "test-task",
				Headers: map[string]string{"task_id": "test-123"},
				Body:    []byte("test-data"),
			},
			retryAttempts:      3,
			serialFailureCount: 0,
			expectError:        false,
		},
		{
			name: "succeed on third attempt",
			task: &worker.Task{
				Type:    "test-task",
				Headers: map[string]string{"task_id": "test-123"},
				Body:    []byte("test-data"),
			},
			retryAttempts:      3,
			serialFailureCount: 2, // Fail twice, succeed on third
			expectError:        false,
		},
		{
			name: "fail after all retry attempts",
			task: &worker.Task{
				Type:    "test-task",
				Headers: map[string]string{"task_id": "test-123"},
				Body:    []byte("test-data"),
			},
			retryAttempts:      2,
			serialFailureCount: 5, // Always fail
			expectError:        true,
			expectedErrMsg:     "failed to publish task after 3 attempts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			bucket := memblob.OpenBucket(nil)
			failureCount := 0
			messageFormat := &mockMessageFormat{
				serializeFunc: func(*worker.Task) ([]byte, error) {
					if failureCount < tt.serialFailureCount {
						failureCount++
						return nil, errors.New("temporary serialization failure")
					}
					return []byte("serialized-data"), nil
				},
			}

			publisher, err := NewPublisher(bucket, messageFormat, WithRetryAttempts(tt.retryAttempts))
			require.NoError(t, err)

			// when
			err = publisher.PublishWithRetry(tt.task)

			// then
			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPublisher_Integration_BasicFlow(t *testing.T) {
	// given
	bucket := memblob.OpenBucket(nil)
	messageFormat := worker.NewBinaryMessageFormat()

	publisher, err := NewPublisher(bucket, messageFormat)
	require.NoError(t, err)

	task := worker.NewTask("integration-test", map[string]string{
		"test-header": "test-value",
	}, []byte("integration test data"))

	// when
	err = publisher.Publish(task)

	// then
	assert.NoError(t, err)

	// Verify the object was stored
	ctx := context.Background()
	iter := bucket.List(&blob.ListOptions{})

	obj, err := iter.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, obj)

	// Verify key format
	assert.Contains(t, obj.Key, task.ID())

	// Verify we can read back the data
	reader, err := bucket.NewReader(ctx, obj.Key, nil)
	require.NoError(t, err)
	defer func() {
		if err := reader.Close(); err != nil {
			logrus.Error("failed to close reader:", err)
		}
	}()

	var buf bytes.Buffer
	_, err = buf.ReadFrom(reader)
	require.NoError(t, err)

	// Verify we can deserialize the data
	deserializedTask, err := messageFormat.Deserialize(buf.Bytes())
	require.NoError(t, err)

	assert.Equal(t, task.Type, deserializedTask.Type)
	assert.Equal(t, task.Headers, deserializedTask.Headers)
	assert.Equal(t, task.Body, deserializedTask.Body)
}
