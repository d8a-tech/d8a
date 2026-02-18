package publishers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/stretchr/testify/assert"
)

// mockPublisher implements Publisher interface for testing
type mockPublisher struct {
	publishCount int32
}

func (m *mockPublisher) Publish(t *worker.Task) error {
	atomic.AddInt32(&m.publishCount, 1)
	return nil
}

// TestNewBackoffPingingPublisher_CreationWithOptions validates publisher creation and option application
func TestNewBackoffPingingPublisher_CreationWithOptions(t *testing.T) {
	// given
	ctx := context.Background()
	mockPub := &mockPublisher{}
	taskFunc := func() (*worker.Task, error) {
		return &worker.Task{}, nil
	}

	// when
	publisher, err := NewBackoffPingingPublisher(
		ctx,
		mockPub,
		taskFunc,
		WithMinInterval(100*time.Millisecond),
		WithMaxInterval(500*time.Millisecond),
		WithIntervalExpFactor(2.0),
	)

	// then
	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	bp, ok := publisher.(*backoffPingingPublisher)
	assert.True(t, ok, "publisher should be *backoffPingingPublisher")
	assert.Equal(t, 100*time.Millisecond, bp.conf.MinInterval)
	assert.Equal(t, 500*time.Millisecond, bp.conf.MaxInterval)
	assert.Equal(t, 2.0, bp.conf.IntervalExpFactor)
}

// TestNewBackoffPingingPublisher_PublishWorks validates publishing tasks works
func TestNewBackoffPingingPublisher_PublishWorks(t *testing.T) {
	// given
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockPub := &mockPublisher{}
	taskFunc := func() (*worker.Task, error) {
		return &worker.Task{}, nil
	}

	publisher, err := NewBackoffPingingPublisher(
		ctx,
		mockPub,
		taskFunc,
		WithMinInterval(10*time.Millisecond),
	)
	assert.NoError(t, err)

	// when
	err = publisher.Publish(&worker.Task{})

	// then
	assert.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockPub.publishCount))

	cancel()
}

// TestNewBackoffPingingPublisher_GeneratesPings validates pings are generated during idle periods
func TestNewBackoffPingingPublisher_GeneratesPings(t *testing.T) {
	// given
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockPub := &mockPublisher{}
	pingCount := int32(0)
	taskFunc := func() (*worker.Task, error) {
		atomic.AddInt32(&pingCount, 1)
		return &worker.Task{}, nil
	}

	publisher, err := NewBackoffPingingPublisher(
		ctx,
		mockPub,
		taskFunc,
		WithMinInterval(10*time.Millisecond),
		WithMaxInterval(50*time.Millisecond),
	)
	assert.NoError(t, err)

	// when - publish one task and wait for pings
	err = publisher.Publish(&worker.Task{})
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// then - verify pings were generated
	assert.Greater(t, atomic.LoadInt32(&pingCount), int32(0))

	cancel()
}
