// Package receiver implements the receiver service.
package receiver

import (
	"fmt"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/sirupsen/logrus"
)

// BatchingStorage is a storage that batches hits and flushes them to a child storage.
type BatchingStorage struct {
	mu          sync.Mutex
	buffer      []*hits.Hit
	child       Storage
	batchSize   int
	timeout     time.Duration
	flushTicker *time.Ticker
	lastFlush   time.Time
	done        chan struct{}
}

// NewBatchingStorage creates a new BatchingStorage instance.
func NewBatchingStorage(child Storage, batchSize int, timeout time.Duration) *BatchingStorage {
	bs := &BatchingStorage{
		child:     child,
		batchSize: batchSize,
		timeout:   timeout,
		done:      make(chan struct{}),
	}
	bs.flushTicker = time.NewTicker(timeout)
	bs.lastFlush = time.Now()

	go func() {
		for {
			select {
			case <-bs.flushTicker.C:
				if time.Since(bs.lastFlush) >= bs.timeout {
					if err := bs.Flush(); err != nil {
						logrus.Errorf("Failed to flush batch: %v", err)
					}
				}
			case <-bs.done:
				return
			}
		}
	}()

	return bs
}

// Push implements the Storage interface
func (bs *BatchingStorage) Push(hits []*hits.Hit) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	bs.buffer = append(bs.buffer, hits...)

	if len(bs.buffer) >= bs.batchSize {
		return bs.flushLocked()
	}
	return nil
}

// Flush flushes the buffer to the child storage.
func (bs *BatchingStorage) Flush() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return bs.flushLocked()
}

func (bs *BatchingStorage) flushLocked() error {
	bs.lastFlush = time.Now()
	if len(bs.buffer) == 0 {
		return nil
	}

	// Copy buffer to avoid mutation during send
	toSend := make([]*hits.Hit, len(bs.buffer))
	copy(toSend, bs.buffer)
	bs.buffer = bs.buffer[:0]

	if err := bs.child.Push(toSend); err != nil {
		logrus.Errorf("Storage push failed: %v", err)
		return fmt.Errorf("failed to push data: %w", err)
	}

	return nil
}

// Close closes the BatchingStorage instance.
func (bs *BatchingStorage) Close() {
	bs.flushTicker.Stop()
	close(bs.done)
	if err := bs.Flush(); err != nil {
		logrus.Errorf("Failed to flush batch: %v", err)
	}
}
