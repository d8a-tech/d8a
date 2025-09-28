package e2e

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/columns/sessioncolumns"
	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protosessions"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/sessions"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
)

type mockReceiverStorage struct {
	pushFunc func(hits []*hits.Hit) error
	receiver.Storage
}

func (m *mockReceiverStorage) Push(hits []*hits.Hit) error {
	return m.pushFunc(hits)
}

type mockCloser struct {
	protosessions.Closer
	closeFunc func([]*hits.Hit) error
}

func (m *mockCloser) Close(hits []*hits.Hit) error {
	return m.closeFunc(hits)
}

func hitIDs(hits []*hits.Hit) []string {
	ids := make([]string, len(hits))
	for i, hit := range hits {
		ids[i] = hit.ID
	}
	sort.Strings(ids)
	return ids
}

func TestProtosessions(t *testing.T) {
	handlerKV := storage.NewInMemoryKV().(*storage.InMemoryKV)            // nolint:forcetypeassert // test code
	handlerSet := storage.NewInMemorySet().(*storage.InMemorySet)         // nolint:forcetypeassert // test code
	sharedSessionStampKV := storage.NewInMemoryKV().(*storage.InMemoryKV) // nolint:forcetypeassert // test code
	receiverStorage := &mockReceiverStorage{}
	lastClosedHits := []*hits.Hit{}
	closer := &mockCloser{
		closeFunc: func(hits []*hits.Hit) error {
			lastClosedHits = hits
			return nil
		},
	}
	closeTrigger := protosessions.NewCloseTriggerMiddleware(
		handlerKV,
		handlerSet,
		5*time.Second,
		1*time.Second,
		closer,
	)
	protosessions.StopCloseTrigger(closeTrigger)
	handler := protosessions.Handler(
		context.Background(),
		handlerSet,
		handlerKV,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
		[]protosessions.Middleware{
			protosessions.NewEvicterMiddleware(sharedSessionStampKV, receiverStorage),
			closeTrigger,
		},
	)
	// re-pushes the evicted hits to the handler we just created
	receiverStorage.pushFunc = func(theHits []*hits.Hit) error {
		err := handler(map[string]string{}, &hits.HitProcessingTask{
			Hits: theHits,
		})
		if err != nil {
			return err
		}
		return nil
	}

	// First hit
	firstHit := hits.New()
	firstHit.IP = "127.0.0.1"
	firstHit.ServerReceivedTime = "2025-01-01T00:00:00Z"
	assert.Nil(t, handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{
			firstHit,
		},
	}))
	assert.Equal(
		t,
		handlerKV.KV[fmt.Sprintf("sessions.expiration.%s", firstHit.AuthoritativeClientID)],
		[]byte("1735689605"),
	)
	all, err := handlerSet.All([]byte("sessions.buckets.1735689605"))
	assert.Nil(t, err)
	assert.Equal(t, all, [][]byte{[]byte(firstHit.AuthoritativeClientID)})
	assert.Len(
		t,
		handlerSet.HM[fmt.Sprintf("sessions.hits.%s", firstHit.AuthoritativeClientID)],
		1,
	)

	// Tick 00
	protosessions.DoTick(closeTrigger)

	// Second hit
	secondHit := hits.New()
	secondHit.IP = "127.0.0.1"
	secondHit.ServerReceivedTime = "2025-01-01T00:00:02Z"
	assert.Nil(t, handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{secondHit},
	}))
	// Should be attributed to the first hit AuthoritativeClientID (session stamp is the same)
	assert.Len(
		t,
		handlerSet.HM[fmt.Sprintf("sessions.hits.%s", firstHit.AuthoritativeClientID)],
		2,
	)
	_, ok := handlerSet.HM[fmt.Sprintf("sessions.hits.%s", secondHit.ClientID)]
	assert.False(t, ok)

	// Tick to 01
	protosessions.DoTick(closeTrigger)

	// Third hit - different proto-session, but should allow progressing the tick (relative time)
	// to a point where the first and second hit are closed
	thirdHit := hits.New()
	thirdHit.IP = "127.0.0.2"
	thirdHit.ServerReceivedTime = "2025-01-01T00:00:10Z"
	assert.Nil(t, handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{thirdHit},
	}))

	// Ticku Ticku up from 01 to 08
	for range 7 {
		protosessions.DoTick(closeTrigger)
	}

	// First and second hit despite different ClientIDs are attributed to the
	// same proto-session (because of session stamp) and closed together
	assert.Equal(t, hitIDs([]*hits.Hit{firstHit, secondHit}), hitIDs(lastClosedHits))

	// The only keys in KVs and Sets are related to the third hit
	assert.Len(
		t,
		handlerKV.KV,
		2,
	)
	for _, expectedKey := range []string{
		"sessions.buckets.next",
		fmt.Sprintf("sessions.expiration.%s", thirdHit.AuthoritativeClientID),
	} {
		_, ok := handlerKV.KV[expectedKey]
		assert.True(t, ok, fmt.Sprintf("expected key %s not found", expectedKey))
	}

	assert.Len(
		t,
		handlerSet.HM,
		2, // sessions.buckets.<bucket for third hit> + sessions.expiration.<third hit AuthoritativeClientID>
	)

	assert.Len(
		t,
		sharedSessionStampKV.KV,
		2,
	)
	for _, expectedKey := range []string{
		"sessions.stamps.127.0.0.2",
		fmt.Sprintf("sessions.stamps.by.client.id.%s", thirdHit.AuthoritativeClientID),
	} {
		_, ok := sharedSessionStampKV.KV[expectedKey]
		assert.True(t, ok, fmt.Sprintf("expected key %s not found", expectedKey))
	}
}

func TestProtosessionsWarehouse(t *testing.T) {
	set := storage.NewInMemorySet()
	kv := storage.NewInMemoryKV()
	closerSessionDuration := 1 * time.Second
	warehouseDriver := &warehouse.MockWarehouseDriver{}
	cmd := protosessions.NewCloseTriggerMiddleware(
		storage.NewMonitoringKV(kv),
		storage.NewMonitoringSet(set),
		closerSessionDuration,
		1*time.Second,
		sessions.NewDirectCloser(
			sessions.NewSessionWriter(
				context.Background(),
				warehouse.NewStaticDriverRegistry(
					warehouse.NewBatchingDriver(
						warehouseDriver,
						1000,
						time.Millisecond*50,
						storage.NewInMemorySet(),
						nil,
					),
				),
				schema.NewStaticColumnsRegistry(
					map[string]schema.Columns{},
					schema.NewColumns(
						[]schema.SessionColumn{
							sessioncolumns.SessionIDColumn,
						},
						[]schema.EventColumn{
							eventcolumns.EventIDColumn,
						},
					),
				),
				schema.NewStaticLayoutRegistry(
					map[string]schema.Layout{},
					schema.NewEmbeddedSessionColumnsLayout(
						"events",
						"session_",
					),
				),
			),
			0,
		),
	)

	handler := protosessions.Handler(
		context.Background(),
		set,
		kv,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
		[]protosessions.Middleware{
			cmd,
		},
	)

	assert.Nil(t, handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(0),
			newHitAfter(1),
		},
	}))

	protosessions.DoTick(cmd)

	assert.Nil(t, handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{
			newHitAfter(2 * time.Second),
		},
	}))
	protosessions.DoTick(cmd)

	// The tick is 50ms, everything should be batched into
	// ~one batch, but depending on timing, it might be two
	assert.Eventually(t, func() bool {
		return warehouseDriver.WriteCallCount < 3 && warehouseDriver.WriteCallCount > 0
	}, 2*time.Second, 10*time.Millisecond)
}

var now = time.Now()

func newHitAfter(d time.Duration) *hits.Hit {
	h := hits.New()
	h.PropertyID = "1337"
	h.ServerReceivedTime = now.Add(d).Format(time.RFC3339)
	return h
}
