// Package bolt provides functionality for storing and retrieving data using BoltDB
package bolt

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

type item struct {
	Key   []byte
	Ts    int64
	Value []byte
}

type consumer struct {
	ctx                  context.Context
	maxItemsToReadAtOnce int
	db                   *bolt.DB
	tasks                chan *worker.Task
	messageFormat        worker.MessageFormat
	nextSleep            time.Duration
}

func (s *consumer) Consume(handler worker.TaskHandlerFunc) error {
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			err := s.processNextBatch(func(tasks []*worker.Task) error {
				errors := []error{}
				for _, task := range tasks {
					err := handler(task)
					if err != nil {
						errors = append(errors, err)
					}
				}
				if len(errors) > 0 {
					return fmt.Errorf("errors handling tasks: %v", errors)
				}
				return nil
			})
			if err != nil {
				logrus.Errorf("Error processing next batch: %v", err)
			}
			time.Sleep(s.nextSleep)
		}
	}
}

// Stream implements transformer.Streamer interface using embedded BoltDB
func (s *consumer) Tasks() chan *worker.Task {
	return s.tasks
}

// processNextBatch handles a single batch of items from the database
func (s *consumer) processNextBatch(cb func([]*worker.Task) error) error {
	tasks := make([]*worker.Task, 0)
	var items []item
	if err := s.db.Update(func(txn *bolt.Tx) error {
		b := txn.Bucket([]byte(TasksBucket))
		if b == nil {
			return fmt.Errorf("bucket %s not found", TasksBucket)
		}
		var err error
		items, err = s.collectItems(b)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}

		processedTasks, err := s.processItems(items)
		if err != nil {
			return err
		}
		tasks = append(tasks, processedTasks...)
		return nil
	}); err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	// We cannot do the processing in a transaction, because it locks write
	// operations from within the handlers (only one transaction can be active at a time).
	// At least once delivery is guaranteed anyway by the current approach.
	if err := cb(tasks); err != nil {
		return err
	}
	return s.db.Update(func(txn *bolt.Tx) error {
		b := txn.Bucket([]byte(TasksBucket))
		if b == nil {
			return fmt.Errorf("bucket %s not found", TasksBucket)
		}
		return s.deleteProcessedItems(b, items)
	})
}

// collectItems gathers all relevant items from the bucket
func (s *consumer) collectItems(b *bolt.Bucket) ([]item, error) {
	var items []item
	c := b.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		if !strings.HasPrefix(string(k), TasksPrefix) {
			continue
		}
		n, err := ParseNano(string(k))
		if err != nil {
			return nil, err
		} // Make copies of k and v as they're only valid during transaction
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)

		valueCopy := make([]byte, len(v))
		copy(valueCopy, v)
		items = append(items, item{
			Key:   keyCopy,
			Ts:    n,
			Value: valueCopy,
		})
		if len(items) > s.maxItemsToReadAtOnce {
			logrus.Infof(
				"Bolt consumer reached max items to read at once (%d), will read more later",
				s.maxItemsToReadAtOnce,
			)
			break
		}
	}

	if len(items) > s.maxItemsToReadAtOnce {
		s.nextSleep = 0
	} else {
		s.nextSleep = time.Millisecond * 10
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Ts < items[j].Ts
	})

	return items, nil
}

// processItems handles the processing of collected items
func (s *consumer) processItems(items []item) ([]*worker.Task, error) {
	tasks := make([]*worker.Task, len(items))
	for i, item := range items {
		task, err := s.messageFormat.Deserialize(item.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize task: %w", err)
		}
		tasks[i] = task
	}
	return tasks, nil
}

// deleteProcessedItems removes processed items from the bucket
func (s *consumer) deleteProcessedItems(b *bolt.Bucket, items []item) error {
	for _, item := range items {
		if err := b.Delete(item.Key); err != nil {
			return err
		}
	}
	return nil
}

// NewConsumer creates a new consumer, using embedded BoltDB
func NewConsumer(ctx context.Context, b *bolt.DB, messageFormat worker.MessageFormat) worker.Consumer {
	c := &consumer{
		ctx: ctx,
		db:  b,
		// This is added to avoid loading all items to memory after longer
		// consumer downtime. May slow catching up with the latest tasks.
		maxItemsToReadAtOnce: 50,
		tasks:                make(chan *worker.Task),
		messageFormat:        messageFormat,
	}
	return c
}
