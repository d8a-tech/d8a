package protosessions

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/pings"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
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
func (c *Context) TriggerCleanup(authoritativeClientID hits.ClientID) error {
	for _, middleware := range c.allMiddlewares {
		err := middleware.OnCleanup(c, authoritativeClientID)
		if err != nil {
			return err
		}
	}
	return c.StorageSet.Delete([]byte(ProtoSessionHitsKey(authoritativeClientID)))
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
	OnCleanup(ctx *Context, authoritativeClientID hits.ClientID) error
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
	return func(md map[string]string, h *hits.HitProcessingTask) *worker.Error {
		if isPing, err := handlePing(md, c, middlewares); isPing {
			return err
		}

		logrus.Infof("Processing hits: %d", len(h.Hits))
		sortHitsByTime(h.Hits)

		for _, hit := range h.Hits {
			nextFuncs := make([]func(context.Context) error, len(middlewares)+1)
			for i := range nextFuncs {
				if i == len(nextFuncs)-1 {
					nextFuncs[i] = func(_ context.Context) error {
						b := bytes.NewBuffer(nil)
						_, err := encoder(b, hit)
						if err != nil {
							return worker.NewError(worker.ErrTypeDroppable, err)
						}
						return set.Add([]byte(ProtoSessionHitsKey(hit.AuthoritativeClientID)), b.Bytes())
					}
				} else {
					nextFuncs[i] = func(ctx context.Context) error {
						return middlewares[i].Handle(c, hit, func() error {
							return nextFuncs[i+1](ctx)
						})
					}
				}
			}
			err := nextFuncs[0](ctx)
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
		timeI, errI := time.Parse(time.RFC3339, hits[i].ServerReceivedTime)
		if errI != nil {
			return false
		}
		timeJ, errJ := time.Parse(time.RFC3339, hits[j].ServerReceivedTime)
		if errJ != nil {
			return false
		}
		return timeI.Before(timeJ)
	})
}
