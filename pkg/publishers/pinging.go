package publishers

import (
	"context"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
)

type pingingPublisher struct {
	publisher worker.Publisher
	lastPing  time.Time
	interval  time.Duration
	taskFunc  func() (*worker.Task, error)
}

// Push implements the Publish interface by writing hits to stdout in JSON format
func (p *pingingPublisher) Publish(task *worker.Task) error {
	p.lastPing = time.Now()
	return p.publisher.Publish(task)
}

func (p *pingingPublisher) tick() {
	if time.Since(p.lastPing) > p.interval {
		p.lastPing = time.Now()
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

// NewPingingPublisher creates a new publisher that pings the given task at the given interval
func NewPingingPublisher(
	ctx context.Context,
	publisher worker.Publisher,
	interval time.Duration,
	taskFunc func() (*worker.Task, error),
) worker.Publisher {
	p := &pingingPublisher{
		publisher: publisher,
		interval:  interval,
		taskFunc:  taskFunc,
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
