package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
)

// objectItem represents an object in storage with metadata for processing.
type objectItem struct {
	Key       string
	Timestamp int64
	Data      []byte
}

// Consumer implements worker.Consumer using object storage via Go CDK blob.Bucket.
type Consumer struct {
	ctx             context.Context
	bucket          *blob.Bucket
	config          *config
	messageFormat   worker.MessageFormat
	currentInterval time.Duration
}

// NewConsumer creates a new object storage consumer that implements worker.Consumer.
// It accepts a context, Go CDK blob.Bucket, message format, and optional configuration.
func NewConsumer(
	ctx context.Context,
	bucket *blob.Bucket,
	messageFormat worker.MessageFormat,
	opts ...Option,
) (*Consumer, error) {
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

	return &Consumer{
		ctx:             ctx,
		bucket:          bucket,
		config:          cfg,
		messageFormat:   messageFormat,
		currentInterval: cfg.MinInterval,
	}, nil
}

// Consume implements worker.Consumer.Consume by continuously polling for objects
// and processing them in FIFO order based on timestamp.
func (c *Consumer) Consume(handler worker.TaskHandlerFunc) error {
	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
			processed, err := c.processNextBatch(handler)
			if err != nil {
				logrus.Errorf("Error processing next batch: %v", err)
			}

			// Sleep between polling attempts (only if no tasks were processed)
			if processed == 0 {
				time.Sleep(c.currentInterval)
				// Apply exponential backoff when nothing to consume
				c.currentInterval = time.Duration(float64(c.currentInterval) * c.config.IntervalExpFactor)
				logrus.Debugf(
					"Object storage consumer exponential backoff: current interval is %s, max interval is %s",
					c.currentInterval,
					c.config.MaxInterval,
				)
				if c.currentInterval > c.config.MaxInterval {
					c.currentInterval = c.config.MaxInterval
				}
			} else {
				// Reset to minimum interval when items are found
				c.currentInterval = c.config.MinInterval
			}
		}
	}
}

// processNextBatch handles a single batch of objects from storage.
func (c *Consumer) processNextBatch(handler worker.TaskHandlerFunc) (int, error) {
	// List objects with the tasks prefix

	items, err := c.listObjects(c.config.Prefix)
	if err != nil {
		return 0, fmt.Errorf("failed to list objects: %w", err)
	}

	if len(items) == 0 {
		return 0, nil // No tasks to process
	}

	// Sort items by timestamp for FIFO processing
	sort.Slice(items, func(i, j int) bool {
		return items[i].Timestamp < items[j].Timestamp
	})

	// Limit the number of items processed at once
	if len(items) > c.config.MaxItemsToReadAtOnce {
		items = items[:c.config.MaxItemsToReadAtOnce]
		logrus.Infof("Object storage consumer limiting batch to %d items", c.config.MaxItemsToReadAtOnce)
	}

	// Download and deserialize tasks
	tasks, err := c.downloadAndDeserializeTasks(items)
	if err != nil {
		return 0, fmt.Errorf("failed to download and deserialize tasks: %w", err)
	}

	// Process tasks
	errors := []error{}
	for _, task := range tasks {
		if err := handler(task); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return 0, fmt.Errorf("errors processing tasks: %v", errors)
	}

	// Clean up processed objects
	if err := c.cleanupProcessedObjects(items); err != nil {
		return 0, fmt.Errorf("failed to cleanup processed objects: %w", err)
	}

	return len(tasks), nil
}

// listObjects retrieves object metadata from storage with the given prefix.
func (c *Consumer) listObjects(prefix string) ([]objectItem, error) {
	var items []objectItem

	prefix = strings.TrimPrefix(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	iter := c.bucket.List(&blob.ListOptions{Prefix: prefix})

	for {
		obj, err := iter.Next(c.ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("error iterating objects: %w", err)
		}

		// Parse timestamp from key for ordering
		timestamp, err := ParseTimestampFromKey(obj.Key)
		if err != nil {
			logrus.Warnf("Failed to parse timestamp from key %s: %v", obj.Key, err)
			continue
		}

		items = append(items, objectItem{
			Key:       obj.Key,
			Timestamp: timestamp,
		})

		// Respect max items limit during listing to prevent memory exhaustion
		if len(items) >= c.config.MaxItemsToReadAtOnce*2 {
			logrus.Infof("Reached max items during listing, will process more later")
			break
		}
	}

	return items, nil
}

// downloadAndDeserializeTasks downloads object data and deserializes them into tasks.
func (c *Consumer) downloadAndDeserializeTasks(items []objectItem) ([]*worker.Task, error) {
	tasks := make([]*worker.Task, 0, len(items))

	for i := range items {
		// Download object data
		reader, err := c.bucket.NewReader(c.ctx, items[i].Key, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader for key %s: %w", items[i].Key, err)
		}

		// Read all data
		data, err := io.ReadAll(reader)
		func() {
			if closeErr := reader.Close(); closeErr != nil {
				logrus.Error("failed to close reader:", closeErr)
			}
		}()
		if err != nil {
			return nil, fmt.Errorf("failed to read data for key %s: %w", items[i].Key, err)
		}

		// Store data in the item for later cleanup reference
		items[i].Data = data

		// Deserialize task
		task, err := c.messageFormat.Deserialize(data)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize task from key %s: %w", items[i].Key, err)
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}

// cleanupProcessedObjects handles cleanup of processed objects by deleting them.
func (c *Consumer) cleanupProcessedObjects(items []objectItem) error {
	for _, item := range items {
		// Delete the object
		if err := c.bucket.Delete(c.ctx, item.Key); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", item.Key, err)
		}
	}

	return nil
}
