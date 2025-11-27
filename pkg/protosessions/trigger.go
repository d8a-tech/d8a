package protosessions

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/monitoring"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type catchUpState struct {
	lastCatchUpLogTime   time.Time
	startingBucketNumber int64
	lastLoggedBucket     int64
}

// The limits below are not confiugurable to protect against abuse of the system.
// The limits are anyway far beyound any reasonable use case.
var maxEventInSessionHardCap uint32 = 10000
var maxTimeInSessionHardCap = 24 * time.Hour

type closeTriggerMiddleware struct {
	lock                    sync.Mutex
	cache                   destinationBucketCache
	kv                      storage.KV
	set                     storage.Set
	protoSessionSizes       sync.Map
	firstHitTime            sync.Map
	sessionDuration         time.Duration
	loopSleepDuration       time.Duration
	lastHandledHitTime      time.Time
	tickInterval            time.Duration
	catchUpState            *catchUpState
	skipCatchingUp          bool
	catchUpBucketsThreshold int64
	catchUpLogInterval      time.Duration
	lastCtx                 *Context
	closer                  Closer
	stop                    bool
	tickHistogram           metric.Float64Histogram
	lagGauge                metric.Float64Gauge
}

type destinationBucketCache interface {
	Get(authoritativeClientID hits.ClientID) (int64, bool)
	Set(authoritativeClientID hits.ClientID, destinationBucket int64)
}

type ristrettoDestinationBucketCache struct {
	cache    *ristretto.Cache[string, int64]
	cacheTTL time.Duration
}

func (c *ristrettoDestinationBucketCache) Get(authoritativeClientID hits.ClientID) (int64, bool) {
	return c.cache.Get(string(authoritativeClientID))
}

func (c *ristrettoDestinationBucketCache) Set(authoritativeClientID hits.ClientID, destinationBucket int64) {
	c.cache.SetWithTTL(
		string(authoritativeClientID),
		destinationBucket,
		1,
		c.cacheTTL,
	)
}

func newRistrettoDestinationBucketCache(cacheTTL time.Duration) (destinationBucketCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, int64]{
		NumCounters: 10000,
		MaxCost:     10000,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &ristrettoDestinationBucketCache{cache, cacheTTL}, nil
}

func (m *closeTriggerMiddleware) Handle(ctx *Context, hit *hits.Hit, next func() error) error {
	serverReceivedTime := hit.ServerReceivedTime
	m.lastCtx = ctx
	logrus.Debugf("Handling hit: %s/%s", hit.AuthoritativeClientID, hit.ID)
	m.lastHandledHitTime = serverReceivedTime
	logrus.Tracef("Setting last handled hit time to: %s", m.lastHandledHitTime)
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

	m.cache.Set(hit.AuthoritativeClientID, expirationTimeBucket)

	// We add the AuthoritativeClientID to the set of AuthoritativeClientIDs that are suspected to be closed
	// in the given bucket.
	err = m.set.Add([]byte(BucketsKey(expirationTimeBucket)), []byte(hit.AuthoritativeClientID))
	if err != nil {
		return err
	}

	var size uint32
	if val, ok := m.protoSessionSizes.Load(hit.AuthoritativeClientID); ok {
		if v, ok := val.(uint32); ok {
			size = v
		}
	}
	size++
	m.protoSessionSizes.Store(hit.AuthoritativeClientID, size)
	_, ok := m.firstHitTime.Load(hit.AuthoritativeClientID)
	if !ok {
		m.firstHitTime.Store(hit.AuthoritativeClientID, hit.ServerReceivedTime)
	}
	var firstTime time.Time
	if val, ok := m.firstHitTime.Load(hit.AuthoritativeClientID); ok {
		if v, ok := val.(time.Time); ok {
			firstTime = v
		}
	}
	if size >= maxEventInSessionHardCap ||
		hit.ServerReceivedTime.Sub(firstTime) >= maxTimeInSessionHardCap {
		logrus.Warnf("Reached hard cap for proto-session %s, closing it", hit.AuthoritativeClientID)
		m.lock.Lock()
		defer m.lock.Unlock()
		return m.doCloseProtosession(hit.AuthoritativeClientID)
	}

	return next()
}

func (m *closeTriggerMiddleware) OnCleanup(_ *Context, allCleanedHits []*hits.Hit) error {
	if len(allCleanedHits) == 0 {
		return nil
	}
	authoritativeClientID := allCleanedHits[0].AuthoritativeClientID
	m.protoSessionSizes.Delete(authoritativeClientID)
	m.firstHitTime.Delete(authoritativeClientID)
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
func (m *closeTriggerMiddleware) tick() error { // nolint:funlen // the function contains a lot of comments
	tickStart := time.Now()
	defer func() {
		m.tickHistogram.Record(context.TODO(), time.Since(tickStart).Seconds())
	}()

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
			shouldClose, err := m.shouldBeClosed(hits.ClientID(authoritativeClientID), nextBucketInt)
			if err != nil {
				return false, err
			}
			if !shouldClose {
				continue
			}
			err = m.doCloseProtosession(hits.ClientID(authoritativeClientID))
			if err != nil {
				return false, err
			}
		}
		return false, nil
	})
}

func (m *closeTriggerMiddleware) shouldBeClosed(
	authoritativeClientID hits.ClientID,
	nextBucketInt int64,
) (bool, error) {
	// AuthoritativeClientID of given proto-session may be also in some future tick buckets
	// (Handle method blindly adds the AuthoritativeClientID to its current buckets and does not clean
	// up the older ones). For that, we are checking the destination bucket, marked separately
	// by Handle.
	var destinationBucketInt int64
	destinationBucketInt, ok := m.cache.Get(authoritativeClientID)
	if !ok {
		destinationBucket, err := m.kv.Get([]byte(ExpirationKey(string(authoritativeClientID))))
		if err != nil {
			return false, fmt.Errorf("failed to get expiration key: %w", err)
		}
		if len(destinationBucket) == 0 {
			// TODO adda  metric for that
			logrus.Debugf(
				"Destination bucket for %s is empty, skipping. Maybe it was evicted?",
				ExpirationKey(string(authoritativeClientID)),
			)
			return false, nil
		}
		destinationBucketInt, err = strconv.ParseInt(string(destinationBucket), 10, 64)
		if err != nil {
			return false, fmt.Errorf(
				"failed to parse destination bucket for %s: %w, (%s)",
				ExpirationKey(string(authoritativeClientID)),
				err,
				string(destinationBucket),
			)
		}
	}
	// If the destination bucket is in the future, we assume, that a future tick
	// will process the proto-session and we skip it for now.
	if destinationBucketInt > nextBucketInt {
		logrus.Debugf("Destination bucket %d is greater than next bucket %d, skipping", destinationBucketInt, nextBucketInt)
		return false, nil
	}
	return true, nil
}

func (m *closeTriggerMiddleware) doCloseProtosession(authoritativeClientID hits.ClientID) error {
	// At this point we are closing the proto-session
	allHits, err := m.lastCtx.CollectAll(authoritativeClientID)
	if err != nil {
		return fmt.Errorf("failed to collect hits: %w", err)
	}
	// Do not call Closer if there's nothing to close (possible only in some half-processed state)
	if len(allHits) > 0 {
		sortedHits, err := m.sorted(allHits)
		if err != nil {
			return fmt.Errorf("failed to sort hits: %w", err)
		}
		startTime := time.Now()
		err = m.closer.Close([][]*hits.Hit{sortedHits})
		logrus.Tracef("Closing session took: %s", time.Since(startTime))
		if err != nil {
			return fmt.Errorf("failed to close session: %w", err)
		}
	}
	// Cleanup the data for given AuthoritativeClientID (ctx calls all middlewares to cleanup any leftover data)
	err = m.lastCtx.TriggerCleanup(allHits)
	if err != nil {
		return fmt.Errorf("failed to trigger cleanup: %w", err)
	}
	return nil
}

func (m *closeTriggerMiddleware) handleCatchUp(nextBucketInt, currentBucket int64) int64 {
	if m.skipCatchingUp {
		forcedNextBucketInt := BucketNumber(m.lastHandledHitTime, m.tickInterval)
		logrus.Infof("Skipping processing of %d buckets", forcedNextBucketInt-nextBucketInt)
		nextBucketInt = forcedNextBucketInt
		m.skipCatchingUp = false
	}
	if nextBucketInt < currentBucket-m.catchUpBucketsThreshold {
		if m.catchUpState == nil {
			m.catchUpState = &catchUpState{
				lastCatchUpLogTime:   time.Now(),
				startingBucketNumber: nextBucketInt,
				lastLoggedBucket:     nextBucketInt,
			}
			logrus.Infof(
				"Starting catching up processing, left buckets: %d. "+
					"If you want to skip, use --closer-skip-catching-up flag.",
				currentBucket-nextBucketInt,
			)
		} else if time.Since(m.catchUpState.lastCatchUpLogTime) > m.catchUpLogInterval {
			bucketsProcessed := nextBucketInt - m.catchUpState.lastLoggedBucket
			timeElapsed := time.Since(m.catchUpState.lastCatchUpLogTime)
			var estimatedTimeRemaining time.Duration
			if bucketsProcessed > 0 && timeElapsed > 0 {
				processingRate := float64(bucketsProcessed) / timeElapsed.Seconds()
				bucketsLeft := currentBucket - nextBucketInt
				estimatedTimeRemaining = time.Duration(float64(bucketsLeft)/processingRate) * time.Second
			}
			m.catchUpState.lastCatchUpLogTime = time.Now()
			m.catchUpState.lastLoggedBucket = nextBucketInt
			if estimatedTimeRemaining > 0 {
				logrus.Infof(
					"Catching up processing, left buckets: %d, estimated time remaining: %s",
					currentBucket-nextBucketInt,
					estimatedTimeRemaining.Round(time.Second),
				)
			} else {
				logrus.Infof(
					"Catching up processing, left buckets: %d",
					currentBucket-nextBucketInt,
				)
			}
		}
	}
	return nextBucketInt
}

func (m *closeTriggerMiddleware) withNextBucket(f func(nextBucket int64) (skip bool, err error)) error {
	var nextBucketInt int64
	nextBucket, err := m.kv.Get([]byte(NextBucketKey))
	if err != nil {
		return err
	}
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
	currentBucket := BucketNumber(m.lastHandledHitTime, m.tickInterval)

	// Record lag as difference between current and next bucket in time units
	lagBuckets := BucketNumber(time.Now().UTC(), m.tickInterval) - nextBucketInt

	lagSeconds := float64(lagBuckets) * m.tickInterval.Seconds()
	m.lagGauge.Record(context.TODO(), lagSeconds)

	if nextBucketInt >= currentBucket {
		m.loopSleepDuration = m.tickInterval
		if m.catchUpState != nil {
			logrus.Info("Catched up processing of proto-sessions")
			m.catchUpState = nil
		}
		logrus.Tracef("Bucket %d is not yet closed, skipping", nextBucketInt)
		return nil
	}
	nextBucketInt = m.handleCatchUp(nextBucketInt, currentBucket)
	skip, err := f(nextBucketInt)
	if err != nil {
		return err
	}
	if !skip {
		_, err = m.kv.Set([]byte(NextBucketKey), []byte(strconv.FormatInt(nextBucketInt+1, 10)))
		if err != nil {
			return fmt.Errorf("failed to set next bucket: %w", err)
		}
		err = m.set.Drop([]byte(BucketsKey(nextBucketInt)))
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
	logrus.Tracef("Setting the current marching bucket to: %d", BucketNumber(t, m.tickInterval))
	m.lastHandledHitTime = t
	m.lastCtx = ctx
	return nil
}

// CloseTriggerMiddlewareOption is a function that can be used to configure the close trigger middleware.
type CloseTriggerMiddlewareOption func(*closeTriggerMiddleware)

// WithCatchUpBucketsThreshold sets the number of buckets to catch up.
func WithCatchUpBucketsThreshold(threshold int64) CloseTriggerMiddlewareOption {
	return func(m *closeTriggerMiddleware) {
		m.catchUpBucketsThreshold = threshold
	}
}

// WithCatchUpLogInterval sets the interval at which the catching up process will be logged.
func WithCatchUpLogInterval(interval time.Duration) CloseTriggerMiddlewareOption {
	return func(m *closeTriggerMiddleware) {
		m.catchUpLogInterval = interval
	}
}

// WithSkipCatchingUp skips the catching up process.
func WithSkipCatchingUp() CloseTriggerMiddlewareOption {
	return func(m *closeTriggerMiddleware) {
		m.skipCatchingUp = true
	}
}

// NewCloseTriggerMiddleware creates a new closer middleware for session management.
func NewCloseTriggerMiddleware(
	kv storage.KV,
	set storage.Set,
	sessionDuration,
	tickInterval time.Duration, // tickInterval controls how often the middleware will check for closed protosessions
	closer Closer,
	options ...CloseTriggerMiddlewareOption,
) Middleware {
	destinationBucketCache, err := newRistrettoDestinationBucketCache(sessionDuration)
	if err != nil {
		logrus.Panicf("failed to create destination bucket cache: %s", err)
	}

	meter := otel.GetMeterProvider().Meter("protosessions")
	tickHistogram, _ := meter.Float64Histogram(
		"protosessions.trigger.tick.duration",
		metric.WithDescription("Duration of tick execution in close trigger middleware"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	lagGauge, _ := meter.Float64Gauge(
		"protosessions.trigger.lagv2",
		metric.WithDescription("Processing lag between current and last handled bucket"),
		metric.WithUnit("s"),
	)

	m := &closeTriggerMiddleware{
		lock:                    sync.Mutex{},
		kv:                      kv,
		set:                     set,
		sessionDuration:         sessionDuration,
		tickInterval:            tickInterval,
		closer:                  closer,
		protoSessionSizes:       sync.Map{},
		firstHitTime:            sync.Map{},
		catchUpBucketsThreshold: 30,
		catchUpLogInterval:      10 * time.Second,
		cache:                   destinationBucketCache,
		tickHistogram:           tickHistogram,
		lagGauge:                lagGauge,
	}
	for _, option := range options {
		option(m)
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

/*

A session should be closed when:

    inactivity limit is reached (30 mins by default, configurable)
    certain number of events is recorded (let's say 1000, configurable)
    certain time passes (let's say 12h, configurable)

*/
