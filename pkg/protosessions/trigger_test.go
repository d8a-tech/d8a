package protosessions

import (
	"bytes"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/stretchr/testify/require"
)

type testCloser struct {
	lastProtoSession []*hits.Hit
}

func (c *testCloser) Close(protosession []*hits.Hit) error {
	c.lastProtoSession = protosession
	return nil
}

func TestCloserTick(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	closer := &testCloser{
		lastProtoSession: []*hits.Hit{},
	}
	mw := NewCloseTriggerMiddleware( // nolint:forcetypeassert // it's a test, if panic, it's a bug
		kv,
		set,
		1*time.Second,
		1*time.Second,
		closer,
	).(*closeTriggerMiddleware)

	firstHit := hits.New()
	firstHit.AuthoritativeClientID = "1337"
	firstHit.ServerReceivedTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	ctx := &Context{
		StorageKV:      kv,
		StorageSet:     set,
		Encoder:        encoding.JSONEncoder,
		Decoder:        encoding.JSONDecoder,
		allMiddlewares: []Middleware{mw},
	}

	// when: handling the first hit
	nextCalled := false
	require.NoError(t, mw.Handle(ctx, firstHit, func() error {
		nextCalled = true
		return nil
	}))

	// then: next handler was called
	require.True(t, nextCalled)

	// when: middleware ticks
	require.NoError(t, mw.tick())

	// and: simulate the first hit being stored (worker logic)
	b := bytes.NewBuffer(nil)
	_, err := ctx.Encoder(b, firstHit)
	require.NoError(t, err)
	err = ctx.StorageSet.Add([]byte(ProtoSessionHitsKey(firstHit.AuthoritativeClientID)), b.Bytes())
	require.NoError(t, err)

	// and: a second hit arrives after expiration window
	secondHit := hits.New()
	secondHit.AuthoritativeClientID = "1338"
	secondHit.ServerReceivedTime = firstHit.ServerReceivedTime.Add(2 * time.Second)

	// when: handling the second hit
	nextCalled = false
	require.NoError(t, mw.Handle(ctx, secondHit, func() error {
		nextCalled = true
		return nil
	}))

	// then: next handler was called
	require.True(t, nextCalled)

	// when: first tick closes the 00 (empty, as session duration is 1) bucket
	require.NoError(t, mw.tick())
	// then: no hits should be closed
	require.Equal(t, []*hits.Hit{}, closer.lastProtoSession)

	// when: second tick closes the 01 bucket, containing the first hit
	require.NoError(t, mw.tick())
	// then: first hit should be closed and processed
	require.Equal(t, []*hits.Hit{firstHit}, closer.lastProtoSession)
}

func TestCloseTriggerMiddleware_Sorted(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	closer := &testCloser{}
	middleware := NewCloseTriggerMiddleware(
		kv,
		set,
		1*time.Second,
		1*time.Second,
		closer,
	)

	// Verify the middleware is the correct type
	mw, ok := middleware.(*closeTriggerMiddleware)
	require.True(t, ok, "middleware should be of type *closeTriggerMiddleware")

	// Create hits with different server received times (intentionally out of order)
	hit1 := hits.New()
	hit1.ID = "hit1"
	hit1.ServerReceivedTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(3 * time.Second)

	hit2 := hits.New()
	hit2.ID = "hit2"
	hit2.ServerReceivedTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(1 * time.Second)

	hit3 := hits.New()
	hit3.ID = "hit3"
	hit3.ServerReceivedTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(2 * time.Second)

	unsortedHits := []*hits.Hit{hit1, hit2, hit3}

	// when
	sortedHits, err := mw.sorted(unsortedHits)

	// then
	require.NoError(t, err)
	require.Len(t, sortedHits, 3)

	// Verify the order is correct (earliest first)
	require.Equal(t, "hit2", sortedHits[0].ID) // 2020-01-01T00:00:01Z
	require.Equal(t, "hit3", sortedHits[1].ID) // 2020-01-01T00:00:02Z
	require.Equal(t, "hit1", sortedHits[2].ID) // 2020-01-01T00:00:03Z

	// Verify the original slice is not modified
	require.Equal(t, "hit1", unsortedHits[0].ID)
	require.Equal(t, "hit2", unsortedHits[1].ID)
	require.Equal(t, "hit3", unsortedHits[2].ID)
}

func TestCloseTriggerMiddleware_SortedEmptyAndSingle(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	closer := &testCloser{}
	middleware := NewCloseTriggerMiddleware(
		kv,
		set,
		1*time.Second,
		1*time.Second,
		closer,
	)

	// Verify the middleware is the correct type
	mw, ok := middleware.(*closeTriggerMiddleware)
	require.True(t, ok, "middleware should be of type *closeTriggerMiddleware")

	// when: empty slice
	sortedEmpty, err := mw.sorted([]*hits.Hit{})

	// then
	require.NoError(t, err)
	require.Empty(t, sortedEmpty)

	// when: single hit
	singleHit := hits.New()
	singleHit.ID = "single"
	singleHit.ServerReceivedTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	sortedSingle, err := mw.sorted([]*hits.Hit{singleHit})

	// then
	require.NoError(t, err)
	require.Len(t, sortedSingle, 1)
	require.Equal(t, "single", sortedSingle[0].ID)
}
