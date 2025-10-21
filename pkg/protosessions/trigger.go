package protosessions

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/sirupsen/logrus"
)

type closeTriggerMiddleware struct {
	lock               sync.Mutex
	kv                 storage.KV
	set                storage.Set
	sessionDuration    time.Duration
	loopSleepDuration  time.Duration
	lastHandledHitTime time.Time
	tickInterval       time.Duration
	lastCtx            *Context
	closer             Closer
	stop               bool
}

func (m *closeTriggerMiddleware) Handle(ctx *Context, hit *hits.Hit, next func() error) error {
	serverReceivedTime := hit.ServerReceivedTime
	m.lastCtx = ctx
	logrus.Debugf("Handling hit: %s/%s", hit.AuthoritativeClientID, hit.ID)
	m.lastHandledHitTime = serverReceivedTime
	expirationTimeBucket := BucketNumber(serverReceivedTime.Add(m.sessionDuration), m.tickInterval)

	// We are setting the bucket calculated for serverReceivedTime as the bucket when the
	// proto-session will expire. This is overridden with every consecutive hit with the same
	// client ID.
	_, err := m.kv.Set([]byte(ExpirationKey(string(hit.AuthoritativeClientID))), []byte(
		fmt.Sprintf("%d", expirationTimeBucket),
	))
	if err != nil {
		return err
	}

	// We add the AuthoritativeClientID to the set of AuthoritativeClientIDs that are suspected to be closed
	// in the given bucket.
	err = m.set.Add([]byte(BucketsKey(expirationTimeBucket)), []byte(hit.AuthoritativeClientID))
	if err != nil {
		return err
	}
	return next()
}

func (m *closeTriggerMiddleware) OnCleanup(_ *Context, authoritativeClientID hits.ClientID) error {
	return m.kv.Delete([]byte(ExpirationKey(string(authoritativeClientID))))
}

func (m *closeTriggerMiddleware) OnCollect(_ *Context, _ hits.ClientID) ([]*hits.Hit, error) {
	return []*hits.Hit{}, nil
}

func (m *closeTriggerMiddleware) loop() error {
	for !m.stop {
		time.Sleep(m.loopSleepDuration)
		err := m.tick()
		if err != nil {
			logrus.Errorf("closeTriggerMiddleware tick failed: %s", err)
		}
	}
	return nil
}

func (m *closeTriggerMiddleware) doStop() {
	m.stop = true
}

// tick performs a single tick of the closeTriggerMiddleware.
// The algorithm is loosely based on "timing wheels"
func (m *closeTriggerMiddleware) tick() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	// Setting the sleep duration to default value, can be overridden under certain conditions later
	m.loopSleepDuration = m.tickInterval

	// We are not clossing sessions if no hits have been transformed yet -
	// our timers are not absolute, but relative to the last processed hit
	if m.lastHandledHitTime.IsZero() {
		logrus.Tracef("No hits passed through protosessions yet, skipping closeTriggerMiddleware tick")
		return nil
	}
	// The "bucket" is a set, containing all the Client IDs of proto-sessions that are suspected to be closed
	// at given tick. The Client IDs are added to the set by the Handle method (outside of ticking loop)
	// The ID of last ticked bucket is stored in KV. If none is found, we create a new one using
	// the current tick.
	return m.withNextBucket(func(nextBucketInt int64) (skipIncrement bool, err error) {
		// We read all the client IDs, that are contained in the set and are suspected to be closed
		// at current tick.
		allAuthoritativeClientIDs, err := m.set.All([]byte(BucketsKey(nextBucketInt)))
		if err != nil {
			return false, fmt.Errorf("failed to get client IDs from bucket: %w", err)
		}

		// We reached the protosession closing phase and as such don't want to sleep before next
		// tick. It's possible, that we have more than one bucket to process here (for example the process
		// was restarted after some downtime). If a next tick doesn't reach this phase it will restore the
		// sleep duration.
		m.loopSleepDuration = 0
		for _, authoritativeClientID := range allAuthoritativeClientIDs {
			// AuthoritativeClientID of given proto-session may be also in some future tick buckets
			// (Handle method blindly adds the AuthoritativeClientID to its current buckets and does not clean
			// up the older ones). For that, we are checking the destination bucket, marked separately
			// by Handle.
			destinationBucket, err := m.kv.Get([]byte(ExpirationKey(string(authoritativeClientID))))
			if err != nil {
				return false, fmt.Errorf("failed to get expiration key: %w", err)
			}
			if len(destinationBucket) == 0 {
				logrus.Warnf(
					"Destination bucket for %s is empty, skipping. Maybe it was evicted?",
					ExpirationKey(string(authoritativeClientID)),
				)
				continue
			}
			destinationBucketInt, err := strconv.ParseInt(string(destinationBucket), 10, 64)
			if err != nil {
				return false, fmt.Errorf(
					"failed to parse destination bucket for %s: %w, (%s)",
					ExpirationKey(string(authoritativeClientID)),
					err,
					string(destinationBucket),
				)
			}
			// If the destination bucket is in the future, we assume, that a future tick
			// will process the proto-session and we skip it for now.
			if destinationBucketInt > nextBucketInt {
				logrus.Debugf("Destination bucket %d is greater than next bucket %d, skipping", destinationBucketInt, nextBucketInt)
				continue
			}
			// At this point we are closing the proto-session
			allHits, err := m.lastCtx.CollectAll(hits.ClientID(authoritativeClientID))
			if err != nil {
				return false, fmt.Errorf("failed to collect hits: %w", err)
			}
			// Do not call Closer if there's nothing to close (possible only in some half-processed state)
			if len(allHits) > 0 {
				sortedHits, err := m.sorted(allHits)
				if err != nil {
					return false, fmt.Errorf("failed to sort hits: %w", err)
				}
				err = m.closer.Close(sortedHits)
				if err != nil {
					return false, fmt.Errorf("failed to close session: %w", err)
				}
			}
			// Cleanup the data for given AuthoritativeClientID (ctx calls all middlewares to cleanup any leftover data)
			err = m.lastCtx.TriggerCleanup(hits.ClientID(authoritativeClientID))
			if err != nil {
				return false, fmt.Errorf("failed to trigger cleanup: %w", err)
			}
		}
		return false, nil
	})
}

func (m *closeTriggerMiddleware) withNextBucket(f func(nextBucket int64) (skip bool, err error)) error {
	nextBucket, err := m.kv.Get([]byte(NextBucketKey))
	if err != nil {
		return err
	}
	var nextBucketInt int64
	if nextBucket != nil {
		nextBucketInt, err = strconv.ParseInt(string(nextBucket), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse next bucket: %w", err)
		}
	} else {
		nextBucketInt, err = m.ensureFirstBucket()
		if err != nil {
			return fmt.Errorf("failed to ensure first bucket: %w", err)
		}
	}
	if nextBucketInt >= BucketNumber(m.lastHandledHitTime, m.tickInterval) {
		m.loopSleepDuration = m.tickInterval
		logrus.Tracef("Bucket %d is not yet closed, skipping", nextBucketInt)
		return nil
	}
	skip, err := f(nextBucketInt)
	if err != nil {
		return err
	}
	if !skip {
		_, err = m.kv.Set([]byte(NextBucketKey), []byte(strconv.FormatInt(nextBucketInt+1, 10)))
		if err != nil {
			return fmt.Errorf("failed to set next bucket: %w", err)
		}
		err = m.set.Delete([]byte(BucketsKey(nextBucketInt)))
		if err != nil {
			logrus.Warnf("Failed to delete bucket: %s", err)
		}
	}
	return nil
}

func (m *closeTriggerMiddleware) ensureFirstBucket() (int64, error) {
	if m.lastHandledHitTime.IsZero() {
		return 0, fmt.Errorf("last transformed event time is zero")
	}
	b := BucketNumber(m.lastHandledHitTime, m.tickInterval)
	_, err := m.kv.Set([]byte(NextBucketKey), []byte(strconv.FormatInt(b, 10)))
	if err != nil {
		return 0, fmt.Errorf("failed to set next bucket: %w", err)
	}
	return b, nil
}

// sorted sorts hits by ServerReceivedTime in ascending order (earliest first)
func (m *closeTriggerMiddleware) sorted(allHits []*hits.Hit) ([]*hits.Hit, error) {
	if len(allHits) <= 1 {
		return allHits, nil
	}

	// Create a copy to avoid mutating the original slice
	sorted := make([]*hits.Hit, len(allHits))
	copy(sorted, allHits)

	// Sort by ServerReceivedTime
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ServerReceivedTime.Before(sorted[j].ServerReceivedTime)
	})

	return sorted, nil
}

func (m *closeTriggerMiddleware) OnPing(ctx *Context, t time.Time) error {
	m.lastHandledHitTime = t
	m.lastCtx = ctx
	return nil
}

// NewCloseTriggerMiddleware creates a new closer middleware for session management.
func NewCloseTriggerMiddleware(
	kv storage.KV,
	set storage.Set,
	sessionDuration,
	tickInterval time.Duration, // tickInterval controls how often the middleware will check for closed protosessions
	closer Closer,
) Middleware {
	m := &closeTriggerMiddleware{
		lock:            sync.Mutex{},
		kv:              kv,
		set:             set,
		sessionDuration: sessionDuration,
		tickInterval:    tickInterval,
		closer:          closer,
	}
	go func() {
		err := m.loop()
		if err != nil {
			logrus.Panicf("Closer loop failed: %s", err)
		}
	}()
	return m
}

// BucketsPrefix is the prefix for session buckets keys.
const BucketsPrefix = "sessions.buckets"

// NextBucketKey is the key for the next session bucket.
const NextBucketKey = "sessions.buckets.next"

// ExpirationKey returns the expiration key for a given client ID.
func ExpirationKey(authoritativeClientID string) string {
	return fmt.Sprintf("sessions.expiration.%s", authoritativeClientID)
}

// BucketsKey returns the key for the buckets of the session.
func BucketsKey(bucketNumber int64) string {
	return fmt.Sprintf("%s.%d", BucketsPrefix, bucketNumber)
}

// BucketsParseNumber parses the bucket number from a bucket key.
func BucketsParseNumber(bucketKey string) (int64, error) {
	parts := strings.Split(bucketKey, ".")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid bucket key: %s", bucketKey)
	}
	return strconv.ParseInt(parts[2], 10, 64)
}

// BucketNumber returns the bucket number for a given time and session duration.
func BucketNumber(time time.Time, tickInterval time.Duration) int64 {
	// A bucket every second
	if tickInterval == 0 {
		return time.Unix()
	}
	return time.Unix() / int64(tickInterval.Seconds())
}
