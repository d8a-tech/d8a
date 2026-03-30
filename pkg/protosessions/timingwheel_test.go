package protosessions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimingWheel_SkipWhenNoEventsProcessed(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: -1,
	}
	processor := func(_ context.Context, _ int64) error {
		t.Fatal("processor should not be called when no events processed")
		return nil
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
	processor := func(_ context.Context, _ int64) error {
		t.Fatal("processor should not be called on initialization")
		return nil
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
	processor := func(_ context.Context, bucketNumber int64) error {
		processedBucket = bucketNumber
		return nil
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
	processor := func(_ context.Context, _ int64) error {
		t.Fatal("processor should not be called when bucket not ready")
		return nil
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	tw.UpdateTime(time.Unix(1000, 0))

	// when
	err := tw.tick(context.Background())

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), backend.nextBucket) // Should not advance
}

func TestTimingWheel_ErrorDoesNotAdvance(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: 1000,
	}
	processor := func(_ context.Context, _ int64) error {
		return errors.New("processor error")
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	tw.UpdateTime(time.Unix(1001, 0))

	// when
	err := tw.tick(context.Background())

	// then
	assert.Error(t, err)
	assert.Equal(t, int64(1000), backend.nextBucket) // Should not advance
}

func TestTimingWheel_SkipCatchUpOnStartup_RebasesPersistedBucket(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: 1000,
	}
	processor := func(_ context.Context, _ int64) error {
		t.Fatal("processor should not be called during startup rebase")
		return nil
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	tw.skipCatchUpOnStartup = true
	tw.UpdateTime(time.Unix(1005, 0))

	// when
	err := tw.tick(context.Background())

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(1005), backend.nextBucket)
	assert.True(t, tw.startupBucketRebased)
}

func TestTimingWheel_SkipCatchUpOnStartup_OnlyRebasesOnce(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: 1000,
	}
	var processedBucket int64 = -1
	processor := func(_ context.Context, bucketNumber int64) error {
		processedBucket = bucketNumber
		return nil
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	tw.skipCatchUpOnStartup = true
	tw.UpdateTime(time.Unix(1005, 0))

	// when
	err := tw.tick(context.Background())
	assert.NoError(t, err)

	tw.UpdateTime(time.Unix(1006, 0))
	err = tw.tick(context.Background())

	// then
	assert.NoError(t, err)
	assert.Equal(t, int64(1005), processedBucket)
	assert.Equal(t, int64(1006), backend.nextBucket)
}

func TestTimingWheel_ConcurrentUpdateTimeAndTick(t *testing.T) {
	// given
	backend := &mockTickerBackend{
		nextBucket: 1000,
	}
	processor := func(_ context.Context, _ int64) error {
		return nil
	}
	tw := NewTimingWheel(backend, 1*time.Second, processor)
	tw.UpdateTime(time.Unix(1001, 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// when — concurrent UpdateTime and tick; the race detector will flag
	// unsynchronized access if the mutex is missing.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			tw.UpdateTime(time.Unix(int64(1002+i), 0))
		}
	}()

	for i := 0; i < 500; i++ {
		_ = tw.tick(ctx)
	}
	<-done

	// then — just verify no panic / race; CurrentTime should be monotonic.
	ct := tw.CurrentTime()
	assert.False(t, ct.IsZero())
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
