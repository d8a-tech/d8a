package protosessions

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/monitoring"
	"github.com/d8a-tech/d8a/pkg/pings"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Context holds the context for protosessions operations.
type Context struct {
	StorageSet     storage.Set
	StorageKV      storage.KV
	Encoder        encoding.EncoderFunc
	Decoder        encoding.DecoderFunc
	allMiddlewares []Middleware
}

// TriggerCleanup triggers cleanup for the given clientID across all middlewares.
func (c *Context) TriggerCleanup(allCleanedHits []*hits.Hit) error {
	for _, middleware := range c.allMiddlewares {
		err := middleware.OnCleanup(c, allCleanedHits)
		if err != nil {
			return err
		}
	}
	if len(allCleanedHits) > 0 {
		authoritativeClientID := allCleanedHits[0].AuthoritativeClientID
		return c.StorageSet.Drop([]byte(ProtoSessionHitsKey(authoritativeClientID)))
	}
	return nil
}

// CollectAll collects all hits for the given clientID from all middlewares and storage.
func (c *Context) CollectAll(authoritativeClientID hits.ClientID) ([]*hits.Hit, error) {
	allHits := make([]*hits.Hit, 0)
	for _, middleware := range c.allMiddlewares {
		hits, err := middleware.OnCollect(c, authoritativeClientID)
		if err != nil {
			return nil, err
		}
		allHits = append(allHits, hits...)
	}
	storageHits, err := c.StorageSet.All([]byte(ProtoSessionHitsKey(authoritativeClientID)))
	if err != nil {
		return nil, err
	}
	for _, hit := range storageHits {
		var decodedHit *hits.Hit
		err = c.Decoder(bytes.NewBuffer(hit), &decodedHit)
		if err != nil {
			return nil, err
		}
		allHits = append(allHits, decodedHit)
	}
	return allHits, nil
}

// Middleware defines an interface for task processing middleware
type Middleware interface {
	Handle(ctx *Context, hit *hits.Hit, next func() error) error
	// OnCleanup is called when the data for proto-session for given clientID should be cleared
	OnCleanup(ctx *Context, allCleanedHits []*hits.Hit) error
	OnCollect(ctx *Context, authoritativeClientID hits.ClientID) ([]*hits.Hit, error)

	OnPing(ctx *Context, pingTimestamp time.Time) error
}

// Handler returns a function that processes hit processing tasks.
func Handler(
	ctx context.Context,
	set storage.Set,
	kv storage.KV,
	encoder encoding.EncoderFunc,
	decoder encoding.DecoderFunc,
	middlewares []Middleware,
) func(_ map[string]string, h *hits.HitProcessingTask) *worker.Error {
	c := &Context{
		StorageSet:     set,
		StorageKV:      kv,
		Encoder:        encoder,
		Decoder:        decoder,
		allMiddlewares: middlewares,
	}

	meter := otel.GetMeterProvider().Meter("protosessions")
	handlerHistogram, _ := meter.Float64Histogram(
		"protosessions.hitprocessing.duration",
		metric.WithDescription("Duration of hit processing handler execution"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	middlewareHistogram, _ := meter.Float64Histogram(
		"protosessions.middleware.duration",
		metric.WithDescription("Duration of middleware execution"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	storageHistogram, _ := meter.Float64Histogram(
		"protosessions.storage.duration",
		metric.WithDescription("Duration of storage operation"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.UsBuckets...),
	)

	return func(md map[string]string, h *hits.HitProcessingTask) *worker.Error {

		if isPing, err := handlePing(md, c, middlewares); isPing {
			return err
		}

		logrus.Infof("Processing hits: %d", len(h.Hits))
		sortHitsByTime(h.Hits)

		for _, hit := range h.Hits {
			hitProcessingStart := time.Now()
			nextFuncs := make([]func(context.Context) error, len(middlewares)+1)
			for i := range nextFuncs {
				if i == len(nextFuncs)-1 {
					nextFuncs[i] = func(_ context.Context) error {
						storageStart := time.Now()
						b := bytes.NewBuffer(nil)
						_, err := encoder(b, hit)
						if err != nil {
							return worker.NewError(worker.ErrTypeDroppable, err)
						}
						err = set.Add([]byte(ProtoSessionHitsKey(hit.AuthoritativeClientID)), b.Bytes())
						storageHistogram.Record(ctx, time.Since(storageStart).Seconds())
						return err
					}
				} else {
					middlewareIdx := i
					nextFuncs[i] = func(ctx context.Context) error {
						middlewareStart := time.Now()
						middlewareName := fmt.Sprintf("%T", middlewares[middlewareIdx])
						err := middlewares[middlewareIdx].Handle(c, hit, func() error {
							return nextFuncs[middlewareIdx+1](ctx)
						})
						middlewareHistogram.Record(ctx, time.Since(middlewareStart).Seconds(),
							monitoring.WithAttributes(attribute.String("middleware", middlewareName)))
						return err
					}
				}
			}
			err := nextFuncs[0](ctx)
			handlerHistogram.Record(ctx, time.Since(hitProcessingStart).Seconds())
			if err != nil {
				logrus.Errorf("Task processing error: %s", err)
			}
		}
		return nil
	}
}

// ProtoSessionHitsPrefix is the prefix for session hits keys.
const ProtoSessionHitsPrefix = "sessions.hits"

// ProtoSessionHitsKey returns the key for the session hits of the given authoritative client ID.
func ProtoSessionHitsKey(authoritativeClientID hits.ClientID) string {
	return fmt.Sprintf("%s.%s", ProtoSessionHitsPrefix, authoritativeClientID)
}

// handlePing processes ping requests. Returns (isPing, *worker.Error).
func handlePing(md map[string]string, c *Context, middlewares []Middleware) (bool, *worker.Error) {
	isPing, pingTimestamp := pings.IsTaskAPing(md)
	if !isPing {
		return false, nil
	}

	for _, middleware := range middlewares {
		err := middleware.OnPing(c, pingTimestamp)
		if err != nil {
			logrus.Errorf("Error on ping: %v", err)
			return true, worker.NewError(worker.ErrTypeDroppable, err)
		}
	}
	return true, nil
}

// sortHitsByTime sorts hits by their ServerReceivedTime in chronological order.
func sortHitsByTime(hits []*hits.Hit) {
	sort.Slice(hits, func(i, j int) bool {
		return hits[i].ServerReceivedTime.Before(hits[j].ServerReceivedTime)
	})
}
