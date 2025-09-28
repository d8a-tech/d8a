package receiver

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/stretchr/testify/assert"
)

// batchingMockStorage implements Storage interface for testing
type batchingMockStorage struct {
	hits   []*hits.Hit
	err    error
	closed bool
	mu     sync.Mutex
}

func (m *batchingMockStorage) Push(hits []*hits.Hit) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.hits = append(m.hits, hits...)
	return nil
}

func (m *batchingMockStorage) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

func TestBatchingStorage_BasicBatching(t *testing.T) {
	// given
	mock := &batchingMockStorage{}
	bs := NewBatchingStorage(mock, 3, 100*time.Millisecond)
	defer bs.Close()

	// when
	testHits := []*hits.Hit{
		hits.New(),
		hits.New(),
	}
	err := bs.Push(testHits)
	assert.NoError(t, err)
	assert.Empty(t, mock.hits, "Hits should not be flushed yet")

	// then
	err = bs.Push([]*hits.Hit{hits.New()})
	assert.NoError(t, err)
	assert.Len(t, mock.hits, 3, "All hits should be flushed")
}

func TestBatchingStorage_TimeoutFlush(t *testing.T) {
	// given
	mock := &batchingMockStorage{}
	bs := NewBatchingStorage(mock, 10, 50*time.Millisecond)
	defer bs.Close()

	testHits := []*hits.Hit{
		hits.New(),
		hits.New(),
	}

	// when
	err := bs.Push(testHits)
	assert.NoError(t, err)
	time.Sleep(60 * time.Millisecond)

	// then
	assert.Len(t, mock.hits, 2, "Hits should be flushed after timeout")
}

func TestBatchingStorage_ManualFlush(t *testing.T) {
	// given
	mock := &batchingMockStorage{}
	bs := NewBatchingStorage(mock, 10, 100*time.Millisecond)
	defer bs.Close()

	testHits := []*hits.Hit{
		hits.New(),
		hits.New(),
	}

	// when
	err := bs.Push(testHits)
	assert.NoError(t, err)
	if err := bs.Flush(); err != nil {
		t.Errorf("Failed to flush batch: %v", err)
	}

	// then
	assert.Len(t, mock.hits, 2, "Hits should be flushed after manual flush")
}

func TestBatchingStorage_ErrorHandling(t *testing.T) {
	// given
	mock := &batchingMockStorage{err: errors.New("storage error")}
	bs := NewBatchingStorage(mock, 3, 100*time.Millisecond)
	defer bs.Close()

	testHits := []*hits.Hit{
		hits.New(),
		hits.New(),
		hits.New(),
	}

	// when
	err := bs.Push(testHits)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to push data")
}

func TestBatchingStorage_Close(t *testing.T) {
	// given
	mock := &batchingMockStorage{}
	bs := NewBatchingStorage(mock, 10, 100*time.Millisecond)

	testHits := []*hits.Hit{
		hits.New(),
		hits.New(),
	}

	// when
	err := bs.Push(testHits)
	assert.NoError(t, err)
	bs.Close()

	// then
	assert.Len(t, mock.hits, 2, "Hits should be flushed on close")
}

func TestBatchingStorage_ConcurrentAccess(t *testing.T) {
	// given
	mock := &batchingMockStorage{}
	bs := NewBatchingStorage(mock, 100, 100*time.Millisecond)
	defer bs.Close()

	var wg sync.WaitGroup

	// when
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				hit := hits.New()
				hit.ID = fmt.Sprintf("%d-%d", id, j)
				err := bs.Push([]*hits.Hit{hit})
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()
	if err := bs.Flush(); err != nil {
		t.Errorf("Failed to flush batch: %v", err)
	}

	// then
	assert.Len(t, mock.hits, 100, "All hits should be processed")
}
