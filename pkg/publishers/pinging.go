package publishers

import (
	"context"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/pings"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
)

type pingingPublisher struct {
	publisher          worker.Publisher
	lastPing           time.Time
	lastNonPingPublish time.Time
	mu                 sync.Mutex
	interval           time.Duration
	taskFunc           func() (*worker.Task, error)
}

// Publish implements the Publish interface by writing hits to stdout in JSON format
func (p *pingingPublisher) Publish(task *worker.Task) error {
	p.mu.Lock()
	isPing, _ := pings.IsTaskAPing(task.Headers)
	if !isPing {
		p.lastNonPingPublish = time.Now()
	}
	p.mu.Unlock()
	return p.publisher.Publish(task)
}

func (p *pingingPublisher) tick() {
	p.mu.Lock()
	timeSinceLastPing := time.Since(p.lastPing)
	timeSinceLastNonPing := time.Since(p.lastNonPingPublish)
	p.mu.Unlock()

	// Only generate ping if:
	// 1. Enough time has passed since last ping
	// 2. No non-ping messages were published during the interval
	if timeSinceLastPing > p.interval && timeSinceLastNonPing >= p.interval {
		p.mu.Lock()
		p.lastPing = time.Now()
		p.mu.Unlock()
		task, err := p.taskFunc()
		if err != nil {
			logrus.Errorf("Failed to create ping task: %v", err)
			return
		}
		if err := p.publisher.Publish(task); err != nil {
			logrus.Errorf("Failed to publish ping task: %v", err)
		}
	}
}

// NewPingingPublisher creates a new publisher that pings the given task at the given interval.
// Pings are only generated when no non-ping messages were published during the interval.
func NewPingingPublisher(
	ctx context.Context,
	publisher worker.Publisher,
	interval time.Duration,
	taskFunc func() (*worker.Task, error),
) worker.Publisher {
	p := &pingingPublisher{
		publisher:          publisher,
		interval:           interval,
		taskFunc:           taskFunc,
		lastNonPingPublish: time.Now(),
	}

	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.tick()
			}
		}
	}()

	return p
}
