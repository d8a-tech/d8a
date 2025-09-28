// Package publishers provides implementations of various publisher strategies
package publishers

import (
	"fmt"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
)

// RetryingWithFallbackStorage implements Storage interface with retry logic and fallback
type RetryingWithFallbackStorage struct {
	primary    worker.Publisher
	fallback   worker.Publisher
	maxRetries uint
	retryDelay time.Duration
}

// NewRetryingWithFallbackPublisher creates a new storage instance with retry and fallback capabilities
func NewRetryingWithFallbackPublisher(
	primary, fallback worker.Publisher,
	maxRetries uint,
	retryDelay time.Duration,
) worker.Publisher {
	return &RetryingWithFallbackStorage{
		primary:    primary,
		fallback:   fallback,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
	}
}

// Publish attempts to publish a task to the primary storage with retries.
// If all retries fail, it falls back to the fallback storage.
func (r *RetryingWithFallbackStorage) Publish(t *worker.Task) error {
	var lastErr error

	// Try primary storage with retries
	for retry := uint(0); retry <= r.maxRetries; retry++ {
		err := r.primary.Publish(t)
		if err == nil {
			return nil
		}
		lastErr = err

		if retry < r.maxRetries {
			logrus.Warnf("Primary storage push failed (attempt %d/%d): %v, retrying in %v...",
				retry+1, r.maxRetries, err, r.retryDelay)
			time.Sleep(r.retryDelay)
			continue
		}
	}

	// If all retries failed, try fallback storage
	logrus.Warnf("All primary storage attempts failed: %v, trying fallback storage", lastErr)

	if err := r.fallback.Publish(t); err != nil {
		return fmt.Errorf("both primary (after %d retries) and fallback storage failed: %w", r.maxRetries, err)
	}

	logrus.Info("Successfully pushed data to fallback storage")
	return nil
}
