package receiver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// batchingMockStorage implements Storage interface for testing
type batchingMockStorage struct {
	hits   []*hits.Hit
	err    error
	closed bool
	mu     sync.Mutex
}

func (m *batchingMockStorage) Push(h []*hits.Hit) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.hits = append(m.hits, h...)
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
	bs, cleanup := NewBatchingStorage(mock, 3, 100*time.Millisecond)
	defer cleanup()

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
	bs, cleanup := NewBatchingStorage(mock, 10, 50*time.Millisecond)
	defer cleanup()

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
	bs, cleanup := NewBatchingStorage(mock, 10, 100*time.Millisecond)
	defer cleanup()

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
	bs, cleanup := NewBatchingStorage(mock, 3, 100*time.Millisecond)
	defer cleanup()

	testHits := []*hits.Hit{
		hits.New(),
		hits.New(),
		hits.New(),
	}

	// when
	err := bs.Push(testHits)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "flushing batch")
}

func TestBatchingStorage_Close(t *testing.T) {
	// given
	mock := &batchingMockStorage{}
	bs, _ := NewBatchingStorage(mock, 10, 100*time.Millisecond)

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
	bs, cleanup := NewBatchingStorage(mock, 100, 100*time.Millisecond)
	defer cleanup()

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

func TestBatchingStorage_DefaultUsesMemoryBackend(t *testing.T) {
	// given
	mock := &batchingMockStorage{}

	// when
	bs, cleanup := NewBatchingStorage(mock, 10, 100*time.Millisecond)
	defer cleanup()

	// then
	_, isMemory := bs.backend.(*memoryBatchingBackend)
	assert.True(t, isMemory, "default backend should be memoryBatchingBackend")
}

func TestBatchingStorage_WithBackendOption(t *testing.T) {
	// given
	mock := &batchingMockStorage{}
	dir := t.TempDir()
	fb := NewFileBatchingBackend(FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "test.json.gz",
	})

	// when
	bs, cleanup := NewBatchingStorage(mock, 10, 100*time.Millisecond, WithBackend(fb))
	defer cleanup()

	// then
	_, isFile := bs.backend.(*fileBatchingBackend)
	assert.True(t, isFile, "backend should be fileBatchingBackend when WithBackend is used")
}

func TestMemoryBackend_DoesNotDropHitsOnChildPushFailure(t *testing.T) {
	// given
	pushErr := errors.New("downstream failure")
	mock := &batchingMockStorage{err: pushErr}
	bs, cleanup := NewBatchingStorage(mock, 3, time.Hour)
	defer cleanup()

	h1 := hits.New()
	h1.ID = "hit-1"
	h2 := hits.New()
	h2.ID = "hit-2"
	h3 := hits.New()
	h3.ID = "hit-3"

	// when — first push reaches threshold, child returns error
	err := bs.Push([]*hits.Hit{h1, h2, h3})

	// then — push error propagated
	assert.Error(t, err)
	assert.Empty(t, mock.hits, "child should not have received any hits")

	// given — clear the error
	mock.mu.Lock()
	mock.err = nil
	mock.mu.Unlock()

	// when — second flush should succeed with the original hits
	err = bs.Flush()
	assert.NoError(t, err)

	// then — all original hits delivered exactly once
	assert.Len(t, mock.hits, 3)
	ids := make([]string, len(mock.hits))
	for i, h := range mock.hits {
		ids[i] = h.ID
	}
	assert.Contains(t, ids, "hit-1")
	assert.Contains(t, ids, "hit-2")
	assert.Contains(t, ids, "hit-3")
}

func TestFileBackend_AppendCreatesFlushFile(t *testing.T) {
	// given
	dir := t.TempDir()
	fb := NewFileBatchingBackend(FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "pending.json.gz",
	})

	h := hits.New()
	h.ID = "durable-hit"

	// when
	err := fb.Append([]*hits.Hit{h})

	// then
	require.NoError(t, err)

	flushPath := filepath.Join(dir, "pending.json.gz")
	_, statErr := os.Stat(flushPath)
	assert.NoError(t, statErr, "flush file should exist after Append")

	recovered, err := readFlushFileForTest(fb)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	assert.Equal(t, "durable-hit", recovered[0].ID)
}

func TestFileBackend_FlushRemovesFileOnSuccess(t *testing.T) {
	// given
	dir := t.TempDir()
	fb := NewFileBatchingBackend(FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "pending.json.gz",
	})

	h := hits.New()
	h.ID = "flush-success"
	require.NoError(t, fb.Append([]*hits.Hit{h}))

	var flushedHits []*hits.Hit

	// when
	err := fb.Flush(func(h []*hits.Hit) error {
		flushedHits = h
		return nil
	})

	// then
	require.NoError(t, err)
	require.Len(t, flushedHits, 1)
	assert.Equal(t, "flush-success", flushedHits[0].ID)

	flushPath := filepath.Join(dir, "pending.json.gz")
	_, statErr := os.Stat(flushPath)
	assert.True(t, os.IsNotExist(statErr), "flush file should be removed after successful flush")
}

func TestFileBackend_FlushLeavesFileOnFailure(t *testing.T) {
	// given
	dir := t.TempDir()
	fb := NewFileBatchingBackend(FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "pending.json.gz",
	})

	h := hits.New()
	h.ID = "flush-fail"
	require.NoError(t, fb.Append([]*hits.Hit{h}))

	// when
	err := fb.Flush(func([]*hits.Hit) error {
		return errors.New("downstream failed")
	})

	// then
	assert.Error(t, err)

	flushPath := filepath.Join(dir, "pending.json.gz")
	_, statErr := os.Stat(flushPath)
	assert.NoError(t, statErr, "flush file should still exist after failed flush")

	recovered, err := readFlushFileForTest(fb)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	assert.Equal(t, "flush-fail", recovered[0].ID)
}

func TestFileBackend_RecoverPreExistingHits(t *testing.T) {
	// given — write hits with one backend instance
	dir := t.TempDir()
	cfg := FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "pending.json.gz",
	}
	fb1 := NewFileBatchingBackend(cfg)

	h1 := hits.New()
	h1.ID = "pre-existing"
	require.NoError(t, fb1.Append([]*hits.Hit{h1}))

	// when — create a new backend instance (simulating restart)
	fb2 := NewFileBatchingBackend(cfg)

	var flushedHits []*hits.Hit
	err := fb2.Flush(func(h []*hits.Hit) error {
		flushedHits = h
		return nil
	})

	// then
	require.NoError(t, err)
	require.Len(t, flushedHits, 1)
	assert.Equal(t, "pre-existing", flushedHits[0].ID)
}

func TestBatchingStorage_FileBackend_RecoverPreExistingHitsAfterRestart(t *testing.T) {
	// given — one batching storage stages hits below threshold and is discarded
	dir := t.TempDir()
	cfg := FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "pending.json.gz",
	}
	child := &batchingMockStorage{}

	bs1, cleanup1 := NewBatchingStorage(child, 10, time.Hour, WithBackend(NewFileBatchingBackend(cfg)))
	t.Cleanup(cleanup1)

	h := hits.New()
	h.ID = "pre-existing-batching-hit"
	require.NoError(t, bs1.Push([]*hits.Hit{h}))

	flushPath := filepath.Join(dir, "pending.json.gz")
	_, statErr := os.Stat(flushPath)
	require.NoError(t, statErr, "flush file should exist before restart recovery")

	// when — a new batching storage starts with the same backend path and child
	bs2, cleanup2 := NewBatchingStorage(child, 10, time.Hour, WithBackend(NewFileBatchingBackend(cfg)))
	defer cleanup2()

	err := bs2.Flush()

	// then — staged hits are recovered through BatchingStorage and file is removed
	require.NoError(t, err)
	require.Len(t, child.hits, 1)
	assert.Equal(t, "pre-existing-batching-hit", child.hits[0].ID)

	_, statErr = os.Stat(flushPath)
	assert.True(t, os.IsNotExist(statErr), "flush file should be removed after successful recovery flush")
}

func TestFileBackend_AppendAccumulatesAcrossMultipleCalls(t *testing.T) {
	// given
	dir := t.TempDir()
	fb := NewFileBatchingBackend(FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "pending.json.gz",
	})

	h1 := hits.New()
	h1.ID = "batch-1"
	h2 := hits.New()
	h2.ID = "batch-2"

	// when
	require.NoError(t, fb.Append([]*hits.Hit{h1}))
	require.NoError(t, fb.Append([]*hits.Hit{h2}))

	// then
	recovered, err := readFlushFileForTest(fb)
	require.NoError(t, err)
	require.Len(t, recovered, 2)
	assert.Equal(t, "batch-1", recovered[0].ID)
	assert.Equal(t, "batch-2", recovered[1].ID)
}

func TestFileBackend_AppendAccumulatesAcrossMultipleCalls_WithNewlineData(t *testing.T) {
	// given
	dir := t.TempDir()
	fb := NewFileBatchingBackend(FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "pending.json.gz",
	})

	h1 := hits.New()
	h1.ID = "batch-newline-1"
	h1.Metadata["note"] = "line1\nline2"
	h1.Request.Body = []byte("first\nsecond")

	h2 := hits.New()
	h2.ID = "batch-newline-2"
	h2.Request.QueryParams.Set("q", "alpha\nbeta")

	// when
	require.NoError(t, fb.Append([]*hits.Hit{h1}))
	require.NoError(t, fb.Append([]*hits.Hit{h2}))

	// then
	recovered, err := readFlushFileForTest(fb)
	require.NoError(t, err)
	require.Len(t, recovered, 2)
	assert.Equal(t, "line1\nline2", recovered[0].Metadata["note"])
	assert.Equal(t, "first\nsecond", string(recovered[0].Request.Body))
	assert.Equal(t, "alpha\nbeta", recovered[1].Request.QueryParams.Get("q"))
}

func TestBatchingStorage_FileBackend_PushFailurePropagates(t *testing.T) {
	// given — file backend with a child that always fails
	dir := t.TempDir()
	fb := NewFileBatchingBackend(FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "pending.json.gz",
	})

	pushErr := errors.New("downstream unavailable")
	mock := &batchingMockStorage{err: pushErr}
	bs, cleanup := NewBatchingStorage(mock, 2, time.Hour, WithBackend(fb))
	defer cleanup()

	h1 := hits.New()
	h1.ID = "fail-1"
	h2 := hits.New()
	h2.ID = "fail-2"

	// when — push enough hits to trigger flush threshold
	err := bs.Push([]*hits.Hit{h1, h2})

	// then — error propagates to caller (request handling)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "flushing batch")

	// and — hits are not lost, flush file still has them
	recovered, readErr := readFlushFileForTest(fb)
	require.NoError(t, readErr)
	require.Len(t, recovered, 2, "hits must survive failed flush")

	// when — clear the error and flush again
	mock.mu.Lock()
	mock.err = nil
	mock.mu.Unlock()

	err = bs.Flush()
	assert.NoError(t, err)
	assert.Len(t, mock.hits, 2)
}

func TestFileBackend_FlushNoopWhenNoFile(t *testing.T) {
	// given
	dir := t.TempDir()
	fb := NewFileBatchingBackend(FileBatchingBackendConfig{
		Dir:           dir,
		FlushFileName: "nonexistent.json.gz",
	})

	// when
	err := fb.Flush(func([]*hits.Hit) error {
		t.Fatal("callback should not be called when no flush file exists")
		return nil
	})

	// then
	assert.NoError(t, err)
}
