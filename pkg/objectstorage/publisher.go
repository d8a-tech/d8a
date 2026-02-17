package objectstorage

import (
	"context"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/worker"
	"gocloud.dev/blob"
)

// Publisher implements worker.Publisher using object storage via Go CDK blob.Bucket.
type Publisher struct {
	bucket        *blob.Bucket
	config        *config
	messageFormat worker.MessageFormat
}

// NewPublisher creates a new object storage publisher that implements worker.Publisher.
// It accepts a Go CDK blob.Bucket for storage abstraction and optional configuration.
func NewPublisher(bucket *blob.Bucket, messageFormat worker.MessageFormat, opts ...Option) (*Publisher, error) {
	if bucket == nil {
		return nil, fmt.Errorf("bucket cannot be nil")
	}

	if messageFormat == nil {
		return nil, fmt.Errorf("messageFormat cannot be nil")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &Publisher{
		bucket:        bucket,
		config:        cfg,
		messageFormat: messageFormat,
	}, nil
}

// Publish implements worker.Publisher.Publish by serializing the task and uploading it
// as an object with a timestamp-based key for FIFO ordering.
func (p *Publisher) Publish(task *worker.Task) error {
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}

	// Serialize the task using the configured message format
	data, err := p.messageFormat.Serialize(task)
	if err != nil {
		return fmt.Errorf("failed to serialize task: %w", err)
	}

	// Generate a timestamp-based key with UUID for collision resistance
	taskID := task.ID()
	if taskID == "" {
		taskID = GenerateTaskID()
	}
	key := GenerateTaskKey(taskID)
	key = joinPrefix(p.config.Prefix, key)

	// Upload the serialized task to object storage
	ctx := context.Background()
	writer, err := p.bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("failed to create writer for key %s: %w", key, err)
	}

	// Write the data
	if _, err := writer.Write(data); err != nil {
		// Ensure writer is closed even on write error
		_ = writer.Close()
		return fmt.Errorf("failed to write task data for key %s: %w", key, err)
	}

	// Close the writer to commit the object
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer for key %s: %w", key, err)
	}

	return nil
}

// PublishWithRetry publishes a task with retry logic based on the configuration.
func (p *Publisher) PublishWithRetry(task *worker.Task) error {
	var lastErr error

	for attempt := 0; attempt <= p.config.RetryAttempts; attempt++ {
		err := p.Publish(task)
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry on the last attempt
		if attempt < p.config.RetryAttempts {
			continue
		}
	}

	return fmt.Errorf("failed to publish task after %d attempts: %w", p.config.RetryAttempts+1, lastErr)
}
