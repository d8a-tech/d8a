package protosessions

import "github.com/d8a-tech/d8a/pkg/hits"

type HitBatch struct {
	batch        []*hits.Hit
	currentIndex int
}

type HitBatchMiddleware interface {
	/*
		Handle(ctx *Context, hit *hits.Hit, next func() error) error
		// OnCleanup is called when the data for proto-session for given clientID should be cleared
		OnCleanup(ctx *Context, allCleanedHits []*hits.Hit) error
		OnCollect(ctx *Context, authoritativeClientID hits.ClientID) ([]*hits.Hit, error)

		OnPing(ctx *Context, pingTimestamp time.Time) error
	*/
}
