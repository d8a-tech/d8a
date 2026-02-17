package publishers

import (
	"context"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/pings"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
)

type ExponentialBackoffConfig struct {
	MinInterval       time.Duration
	IntervalExpFactor float64
	MaxInterval       time.Duration
}

func (c ExponentialBackoffConfig) withDefaults() ExponentialBackoffConfig {
	if c.MinInterval == 0 {
		c.MinInterval = 5 * time.Second
	}
	if c.IntervalExpFactor == 0 {
		c.IntervalExpFactor = 1.5
	}
	if c.MaxInterval == 0 {
		c.MaxInterval = 5 * time.Minute
	}
	return c
}

func (c ExponentialBackoffConfig) Validate() error {
	c = c.withDefaults()
	if c.MinInterval < 0 {
		return &ErrInvalidBackoffConfig{Field: "MinInterval", Message: "must be non-negative"}
	}
	if c.IntervalExpFactor <= 0 {
		return &ErrInvalidBackoffConfig{Field: "IntervalExpFactor", Message: "must be greater than 0"}
	}
	if c.MaxInterval < 0 {
		return &ErrInvalidBackoffConfig{Field: "MaxInterval", Message: "must be non-negative"}
	}
	if c.MinInterval > c.MaxInterval {
		return &ErrInvalidBackoffConfig{Field: "MinInterval", Message: "must be less than or equal to MaxInterval"}
	}
	return nil
}

type ErrInvalidBackoffConfig struct {
	Field   string
	Message string
}

func (e *ErrInvalidBackoffConfig) Error() string {
	return "invalid backoff config field '" + e.Field + "': " + e.Message
}

type backoffPingingPublisher struct {
	publisher worker.Publisher
	conf      ExponentialBackoffConfig
	taskFunc  func() (*worker.Task, error)

	mu                 sync.Mutex
	lastPing           time.Time
	lastNonPingPublish time.Time
	currentInterval    time.Duration
}

// BackoffOption is a functional option for configuring ExponentialBackoffConfig.
type BackoffOption func(*ExponentialBackoffConfig)

// WithMinInterval sets the minimum interval for the backoff.
func WithMinInterval(d time.Duration) BackoffOption {
	return func(c *ExponentialBackoffConfig) {
		c.MinInterval = d
	}
}

// WithMaxInterval sets the maximum interval for the backoff.
func WithMaxInterval(d time.Duration) BackoffOption {
	return func(c *ExponentialBackoffConfig) {
		c.MaxInterval = d
	}
}

// WithIntervalExpFactor sets the exponential growth factor for the backoff.
func WithIntervalExpFactor(factor float64) BackoffOption {
	return func(c *ExponentialBackoffConfig) {
		c.IntervalExpFactor = factor
	}
}

func (p *backoffPingingPublisher) Publish(task *worker.Task) error {
	p.mu.Lock()
	isPing, _ := pings.IsTaskAPing(task.Headers)
	if !isPing {
		now := time.Now()
		p.lastNonPingPublish = now
		p.currentInterval = p.conf.MinInterval
	}
	p.mu.Unlock()
	return p.publisher.Publish(task)
}

func (p *backoffPingingPublisher) loop(ctx context.Context) {
	for {
		p.mu.Lock()
		wait := p.currentInterval
		p.mu.Unlock()

		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}

		now := time.Now()
		p.mu.Lock()
		timeSinceLastPing := now.Sub(p.lastPing)
		timeSinceLastNonPing := now.Sub(p.lastNonPingPublish)
		currentInterval := p.currentInterval
		p.mu.Unlock()

		if timeSinceLastPing > currentInterval && timeSinceLastNonPing >= currentInterval {
			task, err := p.taskFunc()
			if err != nil {
				logrus.Errorf("Failed to create ping task: %v", err)
				continue
			}
			if err := p.publisher.Publish(task); err != nil {
				logrus.Errorf("Failed to publish ping task: %v", err)
				continue
			}

			p.mu.Lock()
			p.lastPing = now
			next := time.Duration(float64(p.currentInterval) * p.conf.IntervalExpFactor)
			if next > p.conf.MaxInterval {
				next = p.conf.MaxInterval
			}
			p.currentInterval = next
			p.mu.Unlock()
		}
	}
}

// NewBackoffPingingPublisher creates a publisher that emits ping tasks only
// while idle, using exponential backoff between pings.
func NewBackoffPingingPublisher(
	ctx context.Context,
	publisher worker.Publisher,
	taskFunc func() (*worker.Task, error),
	opts ...BackoffOption,
) (worker.Publisher, error) {
	conf := ExponentialBackoffConfig{}
	conf = conf.withDefaults()

	// Apply options
	for _, opt := range opts {
		opt(&conf)
	}

	// Reapply defaults for unset values after options
	conf = conf.withDefaults()

	if err := conf.Validate(); err != nil {
		return nil, err
	}
	if publisher == nil {
		return nil, &ErrInvalidBackoffConfig{Field: "publisher", Message: "cannot be nil"}
	}
	if taskFunc == nil {
		return nil, &ErrInvalidBackoffConfig{Field: "taskFunc", Message: "cannot be nil"}
	}

	p := &backoffPingingPublisher{
		publisher:          publisher,
		conf:               conf,
		taskFunc:           taskFunc,
		lastNonPingPublish: time.Now(),
		currentInterval:    conf.MinInterval,
	}

	go p.loop(ctx)
	return p, nil
}
