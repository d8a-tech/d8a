package protosessionsv3

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// BucketNextInstruction indicates how the timing wheel should advance after processing.
type BucketNextInstruction string

const (
	// BucketProcessingAdvance indicates the bucket was processed and wheel should advance.
	BucketProcessingAdvance BucketNextInstruction = "advance"
	// BucketProcessingNoop indicates the bucket processing was skipped, don't advance.
	BucketProcessingNoop BucketNextInstruction = "noop"
)

// BucketProcessorFunc processes a single bucket and returns the advancement instruction.
type BucketProcessorFunc func(ctx context.Context, bucketNumber int64) (BucketNextInstruction, error)

// GetCurrentTimeFunc returns the logical current time for the timing wheel.
type GetCurrentTimeFunc func() time.Time

// TimingWheelStateBackend provides abstract storage for timing wheel state.
type TimingWheelStateBackend interface {
	// GetNextBucket returns the next bucket to process.
	// Returns -1 if no bucket has been processed yet (first run).
	GetNextBucket(ctx context.Context) (int64, error)

	// SaveNextBucket persists the next bucket number to process.
	SaveNextBucket(ctx context.Context, bucketNumber int64) error
}

// TimingWheel implements a timing wheel for scheduling protosession closures.
type TimingWheel struct {
	backend        TimingWheelStateBackend
	tickInterval   time.Duration
	processor      BucketProcessorFunc
	getCurrentTime GetCurrentTimeFunc
	stop           chan struct{}
	stopped        chan struct{}
	loopSleep      time.Duration
}

// NewTimingWheel creates a timing wheel with the given tick interval.
func NewTimingWheel(
	backend TimingWheelStateBackend,
	tickInterval time.Duration,
	processor BucketProcessorFunc,
	getCurrentTime GetCurrentTimeFunc,
) *TimingWheel {
	return &TimingWheel{
		backend:        backend,
		tickInterval:   tickInterval,
		processor:      processor,
		getCurrentTime: getCurrentTime,
		stop:           make(chan struct{}),
		stopped:        make(chan struct{}),
		loopSleep:      tickInterval,
	}
}

// Start begins the timing wheel loop in a goroutine.
func (tw *TimingWheel) Start(ctx context.Context) {
	go tw.loop(ctx)
}

// Stop signals the timing wheel to stop and waits for it to finish.
func (tw *TimingWheel) Stop() {
	close(tw.stop)
	<-tw.stopped
}

func (tw *TimingWheel) loop(ctx context.Context) {
	defer close(tw.stopped)

	ticker := time.NewTicker(tw.loopSleep)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("TimingWheel loop stopped due to context cancellation")
			return
		case <-tw.stop:
			logrus.Info("TimingWheel loop stopped")
			return
		case <-ticker.C:
			if err := tw.tick(ctx); err != nil {
				logrus.Errorf("TimingWheel tick failed: %s", err)
			}
			// Update ticker interval in case loopSleep changed
			if tw.loopSleep > 0 {
				ticker.Reset(tw.loopSleep)
			}
		}
	}
}

// tick performs a single tick of the timing wheel.
func (tw *TimingWheel) tick(ctx context.Context) error {
	// Default to sleeping between ticks
	tw.loopSleep = tw.tickInterval

	currentTime := tw.getCurrentTime()
	if currentTime.IsZero() {
		logrus.Tracef("No events processed yet, skipping timing wheel tick")
		return nil
	}

	nextBucket, err := tw.backend.GetNextBucket(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next bucket: %w", err)
	}

	// First run - initialize to current bucket
	if nextBucket == -1 {
		currentBucket := BucketNumber(currentTime, tw.tickInterval)
		if err := tw.backend.SaveNextBucket(ctx, currentBucket); err != nil {
			return fmt.Errorf("failed to initialize bucket: %w", err)
		}
		logrus.Tracef("Initialized timing wheel at bucket %d", currentBucket)
		return nil
	}

	currentBucket := BucketNumber(currentTime, tw.tickInterval)

	logrus.Debugf("TimingWheel tick next bucket: %d, current bucket: %d", nextBucket, currentBucket)

	// Bucket not yet ready to process
	if nextBucket >= currentBucket {
		logrus.Tracef("Bucket %d is not yet closed, skipping", nextBucket)
		return nil
	}

	// Process bucket
	// Set loopSleep to minimal duration to enable fast catch-up
	tw.loopSleep = time.Nanosecond

	result, err := tw.processor(ctx, nextBucket)
	if err != nil {
		return fmt.Errorf("bucket processor failed: %w", err)
	}

	if result == BucketProcessingNoop {
		return nil
	}

	// Advance to next bucket
	if err := tw.backend.SaveNextBucket(ctx, nextBucket+1); err != nil {
		return fmt.Errorf("failed to save next bucket: %w", err)
	}

	return nil
}

func (tw *TimingWheel) BucketNumber(time time.Time) int64 {
	return BucketNumber(time, tw.tickInterval)
}
