// Package receiver implements the receiver service.
package receiver

import (
	"fmt"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/sirupsen/logrus"
)

// BatchingBackend abstracts the persistence layer used by BatchingStorage
// to stage hits before flushing them to the child storage.
type BatchingBackend interface {
	Append([]*hits.Hit) error
	Flush(func([]*hits.Hit) error) error
	Close() error
}

// BatchingOption configures a BatchingStorage instance.
type BatchingOption func(*BatchingStorage)

// WithBackend sets the backend used by BatchingStorage for staging hits.
// When not provided, an in-memory backend is used.
func WithBackend(backend BatchingBackend) BatchingOption {
	return func(bs *BatchingStorage) {
		bs.backend = backend
	}
}

// BatchingStorage is a storage that batches hits and flushes them to a child storage.
type BatchingStorage struct {
	mu           sync.Mutex
	backend      BatchingBackend
	pendingCount int
	child        Storage
	batchSize    int
	timeout      time.Duration
	flushTicker  *time.Ticker
	lastFlush    time.Time
	done         chan struct{}
	closeOnce    sync.Once
}

// NewBatchingStorage creates a new BatchingStorage instance.
func NewBatchingStorage(
	child Storage,
	batchSize int,
	timeout time.Duration,
	opts ...BatchingOption,
) (storage *BatchingStorage, cleanup func()) {
	bs := &BatchingStorage{
		child:     child,
		batchSize: batchSize,
		timeout:   timeout,
		done:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(bs)
	}

	if bs.backend == nil {
		bs.backend = &memoryBatchingBackend{}
	}

	bs.flushTicker = time.NewTicker(timeout)
	bs.lastFlush = time.Now()

	go func() {
		for {
			select {
			case <-bs.flushTicker.C:
				if time.Since(bs.lastFlush) >= bs.timeout {
					if err := bs.Flush(); err != nil {
						logrus.Errorf("failed to flush batch: %v", err)
					}
				}
			case <-bs.done:
				return
			}
		}
	}()

	return bs, bs.Close
}

// Push implements the Storage interface.
func (bs *BatchingStorage) Push(h []*hits.Hit) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if err := bs.backend.Append(h); err != nil {
		return fmt.Errorf("appending hits to backend: %w", err)
	}

	bs.pendingCount += len(h)

	if bs.pendingCount >= bs.batchSize {
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
	// Reset pendingCount unconditionally so that a transient flush failure
	// does not permanently degenerate batching into per-request flushes.
	// The backend retains unflushed hits on error (memory keeps its buffer,
	// file keeps the file) so they will be delivered on the next successful flush.
	bs.pendingCount = 0

	if err := bs.backend.Flush(bs.child.Push); err != nil {
		return fmt.Errorf("flushing batch: %w", err)
	}

	return nil
}

// Close closes the BatchingStorage instance.
func (bs *BatchingStorage) Close() {
	bs.closeOnce.Do(func() {
		bs.flushTicker.Stop()
		close(bs.done)
		if err := bs.Flush(); err != nil {
			logrus.Errorf("failed to flush batch on close: %v", err)
		}
		if err := bs.backend.Close(); err != nil {
			logrus.Errorf("failed to close batching backend: %v", err)
		}
	})
}
