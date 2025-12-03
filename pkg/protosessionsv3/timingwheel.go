package protosessionsv3

import (
	"context"
	"fmt"
	"sync"
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

// TimingWheelStateBackend provides abstract storage for timing wheel state.
type TimingWheelStateBackend interface {
	// GetNextBucket returns the next bucket to process.
	// Returns -1 if no bucket has been processed yet (first run).
	GetNextBucket(ctx context.Context) (int64, error)

	// SaveNextBucket persists the next bucket number to process.
	SaveNextBucket(ctx context.Context, bucketNumber int64) error
}

var mapMutex = sync.Mutex{}

type PerBucketMutexes map[int64]*sync.Mutex

func (m PerBucketMutexes) Lock(bucketNumber int64) {
	mapMutex.Lock()
	mutex, ok := m[bucketNumber]
	if !ok {
		mutex = &sync.Mutex{}
		m[bucketNumber] = mutex
	}
	mapMutex.Unlock()
	mutex.Lock()
}

func (m PerBucketMutexes) TryLock(bucketNumber int64) bool {
	mapMutex.Lock()
	mutex, ok := m[bucketNumber]
	if !ok {
		mutex = &sync.Mutex{}
		m[bucketNumber] = mutex
	}
	mapMutex.Unlock()
	return mutex.TryLock()
}

func (m PerBucketMutexes) Drop(bucketNumber int64) {
	mapMutex.Lock()
	mutex, ok := m[bucketNumber]
	if !ok {
		mapMutex.Unlock()
		return
	}
	delete(m, bucketNumber)
	mapMutex.Unlock()
	mutex.Unlock()
}

// TimingWheel implements a timing wheel for scheduling protosession closures.
type TimingWheel struct {
	backend          TimingWheelStateBackend
	tickInterval     time.Duration
	processor        BucketProcessorFunc
	currentTime      time.Time
	firstUpdatedTime time.Time
	stop             chan struct{}
	stopped          chan struct{}
	loopSleep        time.Duration
	lock             PerBucketMutexes
}

// NewTimingWheel creates a timing wheel with the given tick interval.
func NewTimingWheel(
	backend TimingWheelStateBackend,
	tickInterval time.Duration,
	processor BucketProcessorFunc,
) *TimingWheel {
	return &TimingWheel{
		backend:      backend,
		tickInterval: tickInterval,
		processor:    processor,
		stop:         make(chan struct{}),
		stopped:      make(chan struct{}),
		loopSleep:    tickInterval,
		lock:         make(PerBucketMutexes),
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
			logrus.Debugf("TimingWheel loop stopped due to context cancellation")
			return
		case <-tw.stop:
			logrus.Debugf("TimingWheel loop stopped")
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

	if tw.currentTime.IsZero() {
		logrus.Debugf("No hits processed yet, skipping timing wheel tick")
		return nil
	}

	nextBucket, err := tw.backend.GetNextBucket(ctx)
	if err != nil {
		return fmt.Errorf("failed to get next bucket: %w", err)
	}

	// First run - initialize to current bucket
	if nextBucket == -1 {
		currentBucket := tw.BucketNumber(tw.firstUpdatedTime)
		if err := tw.backend.SaveNextBucket(ctx, currentBucket); err != nil {
			return fmt.Errorf("failed to initialize bucket: %w", err)
		}
		logrus.Tracef("Initialized timing wheel at bucket %d", currentBucket)
		return nil
	}

	currentBucket := tw.BucketNumber(tw.currentTime)
	tw.lock.Lock(currentBucket)
	defer tw.lock.Drop(currentBucket)

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

// UpdateTime updates the timing wheel's current time if the new time is after the existing time.
func (tw *TimingWheel) UpdateTime(t time.Time) {
	if t.After(tw.currentTime) {
		if tw.firstUpdatedTime.IsZero() {
			tw.firstUpdatedTime = t
		}
		tw.currentTime = t
	}
}

// CurrentTime returns the timing wheel's current time.
func (tw *TimingWheel) CurrentTime() time.Time {
	return tw.currentTime
}

func (tw *TimingWheel) BucketNumber(theTime time.Time) int64 {
	// A bucket every second
	if tw.tickInterval < 1 {
		return theTime.Unix()
	}
	return theTime.Unix() / int64(tw.tickInterval.Seconds())
}
