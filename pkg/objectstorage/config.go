// Package objectstorage provides configuration and utilities for object storage operations.
package objectstorage

import (
	"time"
)

// config holds configuration parameters for object storage operations.
type config struct {
	// Prefix is an object key prefix used to namespace queue objects.
	// It should not start with a leading slash.
	Prefix string

	// MaxItemsToReadAtOnce limits the number of objects retrieved in one operation
	// to prevent memory exhaustion during catch-up scenarios
	MaxItemsToReadAtOnce int

	// MinInterval is the minimum duration for exponential backoff polling interval
	MinInterval time.Duration

	// IntervalExpFactor is the exponential factor to multiply the interval by when no tasks are found
	IntervalExpFactor float64

	// MaxInterval is the maximum duration for exponential backoff polling interval
	MaxInterval time.Duration

	// ProcessingTimeout is the maximum time allowed for processing a batch of tasks
	ProcessingTimeout time.Duration

	// RetryAttempts is the number of times to retry failed operations
	RetryAttempts int
}

// defaultConfig returns a configuration with sensible defaults.
func defaultConfig() *config {
	return &config{
		Prefix:               "",
		MaxItemsToReadAtOnce: 1000,
		MinInterval:          time.Second * 5,
		IntervalExpFactor:    1.5,
		MaxInterval:          time.Minute * 1,
		ProcessingTimeout:    time.Minute * 5,
		RetryAttempts:        3,
	}
}

// Option is a function that configures the object storage config.
type Option func(*config)

// WithPrefix sets the object key prefix.
func WithPrefix(prefix string) Option {
	return func(c *config) {
		c.Prefix = prefix
	}
}

// WithMaxItemsToReadAtOnce sets the maximum number of items to read in one batch.
func WithMaxItemsToReadAtOnce(maxItems int) Option {
	return func(c *config) {
		c.MaxItemsToReadAtOnce = maxItems
	}
}

// WithProcessingTimeout sets the processing timeout for task batches.
func WithProcessingTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.ProcessingTimeout = timeout
	}
}

// WithRetryAttempts sets the number of retry attempts for failed operations.
func WithRetryAttempts(attempts int) Option {
	return func(c *config) {
		c.RetryAttempts = attempts
	}
}

// WithMinInterval sets the minimum duration for exponential backoff polling interval.
func WithMinInterval(interval time.Duration) Option {
	return func(c *config) {
		c.MinInterval = interval
	}
}

// WithIntervalExpFactor sets the exponential factor to multiply the interval by when no tasks are found.
func WithIntervalExpFactor(factor float64) Option {
	return func(c *config) {
		c.IntervalExpFactor = factor
	}
}

// WithMaxInterval sets the maximum duration for exponential backoff polling interval.
func WithMaxInterval(interval time.Duration) Option {
	return func(c *config) {
		c.MaxInterval = interval
	}
}

// Validate checks if the configuration values are valid.
func (c *config) Validate() error {
	if c.MaxItemsToReadAtOnce <= 0 {
		return errInvalidConfig{Field: "MaxItemsToReadAtOnce", Message: "must be greater than 0"}
	}

	if c.MinInterval < 0 {
		return errInvalidConfig{Field: "MinInterval", Message: "must be non-negative"}
	}

	if c.IntervalExpFactor <= 0 {
		return errInvalidConfig{Field: "IntervalExpFactor", Message: "must be greater than 0"}
	}

	if c.MaxInterval < 0 {
		return errInvalidConfig{Field: "MaxInterval", Message: "must be non-negative"}
	}

	if c.MinInterval > c.MaxInterval {
		return errInvalidConfig{Field: "MinInterval", Message: "must be less than or equal to MaxInterval"}
	}

	if c.ProcessingTimeout <= 0 {
		return errInvalidConfig{Field: "ProcessingTimeout", Message: "must be greater than 0"}
	}

	if c.RetryAttempts < 0 {
		return errInvalidConfig{Field: "RetryAttempts", Message: "must be non-negative"}
	}

	return nil
}

// errInvalidConfig is returned when configuration validation fails.
type errInvalidConfig struct {
	Field   string
	Message string
}

func (e errInvalidConfig) Error() string {
	return "invalid config field '" + e.Field + "': " + e.Message
}
