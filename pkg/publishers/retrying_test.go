package publishers

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/stretchr/testify/assert"
)

// retryingMockPublisher implements Publisher interface for testing
type retryingMockPublisher struct {
	t         *worker.Task
	err       error
	callCount int
	mu        sync.Mutex
}

func (m *retryingMockPublisher) Publish(t *worker.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t = t
	m.callCount++
	return m.err
}

func TestRetryingWithFallbackStorage_Push(t *testing.T) {
	// given
	tests := []struct {
		name            string
		primaryStorage  *retryingMockPublisher
		fallbackStorage *retryingMockPublisher
		maxRetries      uint
		retryDelay      time.Duration
		task            *worker.Task
		expectedError   error
		expectedCalls   struct {
			primary  int
			fallback int
		}
		expectPanic bool
	}{
		{
			name: "success on first try",
			primaryStorage: &retryingMockPublisher{
				err: nil,
			},
			fallbackStorage: &retryingMockPublisher{
				err: errors.New("should not be called"),
			},
			maxRetries:    3,
			retryDelay:    time.Millisecond,
			task:          &worker.Task{},
			expectedError: nil,
			expectedCalls: struct {
				primary  int
				fallback int
			}{
				primary:  1,
				fallback: 0,
			},
		},
		{
			name: "success after retries",
			primaryStorage: &retryingMockPublisher{
				err: errors.New("temporary error"),
			},
			fallbackStorage: &retryingMockPublisher{
				err: nil,
			},
			maxRetries:    2,
			retryDelay:    time.Millisecond,
			task:          &worker.Task{},
			expectedError: nil,
			expectedCalls: struct {
				primary  int
				fallback int
			}{
				primary:  3, // initial try + 2 retries
				fallback: 1,
			},
		},
		{
			name: "fallback success after all primary retries fail",
			primaryStorage: &retryingMockPublisher{
				err: errors.New("permanent error"),
			},
			fallbackStorage: &retryingMockPublisher{
				err: nil,
			},
			maxRetries:    2,
			retryDelay:    time.Millisecond,
			task:          &worker.Task{},
			expectedError: nil,
			expectedCalls: struct {
				primary  int
				fallback int
			}{
				primary:  3, // initial try + 2 retries
				fallback: 1,
			},
		},
		{
			name: "both primary and fallback fail after retries",
			primaryStorage: &retryingMockPublisher{
				err: errors.New("primary error"),
			},
			fallbackStorage: &retryingMockPublisher{
				err: errors.New("fallback error"),
			},
			maxRetries:    2,
			retryDelay:    time.Millisecond,
			task:          &worker.Task{},
			expectedError: errors.New("both primary (after 2 retries) and fallback storage failed: fallback error"),
			expectedCalls: struct {
				primary  int
				fallback int
			}{
				primary:  3, // initial try + 2 retries
				fallback: 1,
			},
		},
		{
			name: "nil hits slice",
			primaryStorage: &retryingMockPublisher{
				err: nil,
			},
			fallbackStorage: &retryingMockPublisher{
				err: errors.New("should not be called"),
			},
			maxRetries:    2,
			retryDelay:    time.Millisecond,
			task:          nil,
			expectedError: nil,
			expectedCalls: struct {
				primary  int
				fallback int
			}{
				primary:  1,
				fallback: 0,
			},
		},
		{
			name: "zero retry delay",
			primaryStorage: &retryingMockPublisher{
				err: errors.New("temporary error"),
			},
			fallbackStorage: &retryingMockPublisher{
				err: nil,
			},
			maxRetries:    2,
			retryDelay:    0,
			task:          &worker.Task{},
			expectedError: nil,
			expectedCalls: struct {
				primary  int
				fallback int
			}{
				primary:  3,
				fallback: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			if tt.expectPanic {
				assert.Panics(t, func() {
					NewRetryingWithFallbackPublisher(
						tt.primaryStorage,
						tt.fallbackStorage,
						tt.maxRetries,
						tt.retryDelay,
					)
				})
				return
			}

			storage := NewRetryingWithFallbackPublisher(
				tt.primaryStorage,
				tt.fallbackStorage,
				tt.maxRetries,
				tt.retryDelay,
			)

			// when
			err := storage.Publish(tt.task)

			// then
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectedCalls.primary, tt.primaryStorage.callCount)
			assert.Equal(t, tt.expectedCalls.fallback, tt.fallbackStorage.callCount)
			assert.Equal(t, tt.task, tt.primaryStorage.t)
			if tt.fallbackStorage.callCount > 0 {
				assert.Equal(t, tt.task, tt.fallbackStorage.t)
			}
		})
	}
}

func TestRetryingWithFallbackStorage_RetryDelay(t *testing.T) {
	// given
	primaryStorage := &retryingMockPublisher{
		err: errors.New("temporary error"),
	}
	fallbackStorage := &retryingMockPublisher{
		err: nil,
	}
	retryDelay := 50 * time.Millisecond
	var maxRetries uint = 1

	storage := NewRetryingWithFallbackPublisher(
		primaryStorage,
		fallbackStorage,
		maxRetries,
		retryDelay,
	)

	// when
	start := time.Now()
	err := storage.Publish(&worker.Task{})
	duration := time.Since(start)

	// then
	assert.NoError(t, err)
	assert.Equal(t, 2, primaryStorage.callCount) // initial try + 1 retry
	assert.Equal(t, 1, fallbackStorage.callCount)
	assert.GreaterOrEqual(t, duration, retryDelay)
}

func TestRetryingWithFallbackStorage_ConcurrentAccess(t *testing.T) {
	// given
	primaryStorage := &retryingMockPublisher{
		err: errors.New("temporary error"),
	}
	fallbackStorage := &retryingMockPublisher{
		err: nil,
	}
	storage := NewRetryingWithFallbackPublisher(
		primaryStorage,
		fallbackStorage,
		2,
		time.Millisecond,
	)

	// when
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := storage.Publish(&worker.Task{})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// then
	assert.Equal(t, 15, primaryStorage.callCount) // 5 goroutines * (initial try + 2 retries)
	assert.Equal(t, 5, fallbackStorage.callCount) // 5 goroutines
}
