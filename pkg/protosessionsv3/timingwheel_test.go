package protosessionsv3

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimingWheel_SkipWhenNoEventsProcessed(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: -1,
	}
	processor := func(_ context.Context, _ int64) (BucketNextInstruction, error) {
		t.Fatal("processor should not be called when no events processed")
		return BucketProcessingNoop, nil
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	// Don't call UpdateTime - simulates no events processed

	// when
	err := tw.tick(context.Background())

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), backend.nextBucket) // Should not initialize
}

func TestTimingWheel_InitializeOnFirstRun(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: -1,
	}
	processor := func(_ context.Context, _ int64) (BucketNextInstruction, error) {
		t.Fatal("processor should not be called on initialization")
		return BucketProcessingNoop, nil
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	tw.UpdateTime(time.Unix(1000, 0))

	// when
	err := tw.tick(context.Background())

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), backend.nextBucket)
}

func TestTimingWheel_ProcessBucketWhenReady(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: 1000,
	}
	var processedBucket int64
	processor := func(_ context.Context, bucketNumber int64) (BucketNextInstruction, error) {
		processedBucket = bucketNumber
		return BucketProcessingAdvance, nil
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	tw.UpdateTime(time.Unix(1001, 0))

	// when
	err := tw.tick(context.Background())

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), processedBucket)
	assert.Equal(t, int64(1001), backend.nextBucket)
}

func TestTimingWheel_SkipWhenBucketNotReady(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: 1000,
	}
	processor := func(_ context.Context, _ int64) (BucketNextInstruction, error) {
		t.Fatal("processor should not be called when bucket not ready")
		return BucketProcessingNoop, nil
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	tw.UpdateTime(time.Unix(1000, 0))

	// when
	err := tw.tick(context.Background())

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), backend.nextBucket) // Should not advance
}

func TestTimingWheel_NoopDoesNotAdvance(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: 1000,
	}
	processor := func(_ context.Context, _ int64) (BucketNextInstruction, error) {
		return BucketProcessingNoop, nil
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	tw.UpdateTime(time.Unix(1001, 0))

	// when
	err := tw.tick(context.Background())

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), backend.nextBucket) // Should not advance
}

type mockTickerBackend struct {
	nextBucket int64
	err        error
}

func (m *mockTickerBackend) GetNextBucket(_ context.Context) (int64, error) {
	return m.nextBucket, m.err
}

func (m *mockTickerBackend) SaveNextBucket(_ context.Context, bucketNumber int64) error {
	if m.err != nil {
		return m.err
	}
	m.nextBucket = bucketNumber
	return nil
}
