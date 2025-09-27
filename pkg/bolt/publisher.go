// Package bolt provides functionality for storing and retrieving data using BoltDB
package bolt

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/worker"
	bolt "go.etcd.io/bbolt"
)

type publisher struct {
	db            *bolt.DB
	messageFormat worker.MessageFormat
}

// Publish implements the worker.Publisher interface.
func (p *publisher) Publish(t *worker.Task) error {
	data, err := p.messageFormat.Serialize(t)
	if err != nil {
		return fmt.Errorf("failed to serialize task: %w", err)
	}

	return p.db.Update(func(txn *bolt.Tx) error {
		b := txn.Bucket([]byte(TasksBucket))
		if b == nil {
			return fmt.Errorf("bucket %q not found", TasksBucket)
		}
		err := b.Put([]byte(TasksCurrentNanosecondTs()), data)
		return err
	})
}

// NewPublisher creates a new Bolt publisher instance.
func NewPublisher(b *bolt.DB, messageFormat worker.MessageFormat) worker.Publisher {
	return &publisher{
		db:            b,
		messageFormat: messageFormat,
	}
}
