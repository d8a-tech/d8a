package e2e

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/bolt"
	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/columns/sessioncolumns"
	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protosessionsv3"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/sessions"
	"github.com/d8a-tech/d8a/pkg/splitter"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bbolt "go.etcd.io/bbolt"
)

type mockReceiverStorage struct {
	pushFunc func(hits []*hits.Hit) error
	receiver.Storage
}

func (m *mockReceiverStorage) Push(hits []*hits.Hit) error {
	return m.pushFunc(hits)
}

type mockCloser struct {
	protosessionsv3.Closer
	closeFunc func([][]*hits.Hit) error
}

func (m *mockCloser) Close(protosessions [][]*hits.Hit) error {
	return m.closeFunc(protosessions)
}

func hitIDs(theHits []*hits.Hit) []string {
	ids := make([]string, len(theHits))
	for i, hit := range theHits {
		ids[i] = hit.ID
	}
	sort.Strings(ids)
	return ids
}

func TestProtosessionsV3(t *testing.T) { //nolint:funlen // test
	// given
	protoBackendDB, err := bbolt.Open(createTempFile(t), 0o600, nil)
	require.NoError(t, err)
	defer func() { _ = protoBackendDB.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	boltBackend, err := bolt.NewBatchedProtosessionsIOBackend(
		protoBackendDB,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
	)
	require.NoError(t, err)

	kv := storage.NewInMemoryKV()
	lastClosedHitBatches := [][]*hits.Hit{}
	closer := &mockCloser{
		closeFunc: func(protosessions [][]*hits.Hit) error {
			lastClosedHitBatches = append(lastClosedHitBatches, protosessions...)
			return nil
		},
	}

	requeuer := &mockReceiverStorage{
		pushFunc: func(_ []*hits.Hit) error { return nil },
	}
	settingsRegistry := properties.NewTestSettingRegistry()

	baseTime := time.Now().UTC()
	handler := protosessionsv3.Handler(
		ctx,
		protosessionsv3.NewDeduplicatingBatchedIOBackend(boltBackend),
		protosessionsv3.NewGenericStorageTimingWheelBackend("default", kv),
		closer,
		requeuer,
		settingsRegistry,
		protosessionsv3.RewriteIDAndUpdateInPlaceStrategy,
	)

	// First hit
	firstHit := hits.New()
	firstHit.PropertyID = "test-property"
	firstHit.IP = "127.0.0.1"
	firstHit.ServerReceivedTime = baseTime

	// when
	workerErr := handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{firstHit},
	})

	// then
	assert.Nil(t, workerErr)

	// Second hit - same IP (session stamp), should be grouped with first
	secondHit := hits.New()
	secondHit.PropertyID = "test-property"
	secondHit.IP = "127.0.0.1"
	secondHit.ServerReceivedTime = baseTime.Add(2 * time.Second)

	workerErr = handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{secondHit},
	})
	assert.Nil(t, workerErr)

	// Third hit - different IP, different session
	thirdHit := hits.New()
	thirdHit.PropertyID = "test-property"
	thirdHit.IP = "127.0.0.2"
	thirdHit.ServerReceivedTime = baseTime.Add(35 * time.Second)

	workerErr = handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{thirdHit},
	})
	assert.Nil(t, workerErr)

	require.Nil(
		t, protosessionsv3.SendPingWithTime(handler, thirdHit.ServerReceivedTime.Add(60*time.Second)),
	)

	// Wait for timing wheel to process expired buckets
	require.Eventually(t, func() bool {
		return len(lastClosedHitBatches) == 2
	}, 5*time.Second, 100*time.Millisecond, "expected exactly two proto-sessions to close")

	// Verify first and second hit are closed together (same session stamp)
	found := false
	for _, closedSession := range lastClosedHitBatches {
		ids := hitIDs(closedSession)
		expectedIDs := hitIDs([]*hits.Hit{firstHit, secondHit})
		if len(ids) == len(expectedIDs) && ids[0] == expectedIDs[0] && ids[1] == expectedIDs[1] {
			found = true
			break
		}
	}
	assert.True(t, found, "expected first and second hit to be closed in same session")
}

func TestProtosessionsV3Warehouse(t *testing.T) { //nolint:funlen // test
	// given
	protoBackendDB, err := bbolt.Open(createTempFile(t), 0o600, nil)
	require.NoError(t, err)
	defer func() { _ = protoBackendDB.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	boltBackend, err := bolt.NewBatchedProtosessionsIOBackend(
		protoBackendDB,
		encoding.JSONEncoder,
		encoding.JSONDecoder,
	)
	require.NoError(t, err)

	kv := storage.NewInMemoryKV()
	warehouseDriver := &warehouse.MockWarehouseDriver{}
	requeuer := &mockReceiverStorage{
		pushFunc: func(_ []*hits.Hit) error { return nil },
	}
	settingsRegistry := properties.NewTestSettingRegistry()

	sessionWriter := sessions.NewSessionWriter(
		ctx,
		warehouse.NewStaticDriverRegistry(
			warehouse.NewBatchingDriver(
				ctx,
				warehouseDriver,
				1000,
				time.Millisecond*50,
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
				[]schema.SessionScopedEventColumn{},
			),
		),
		schema.NewStaticLayoutRegistry(
			map[string]schema.Layout{},
			schema.NewEmbeddedSessionColumnsLayout("events", "session_"),
		),
		splitter.NewStaticRegistry(splitter.NewNoop()),
	)
	closer := sessions.NewDirectCloser(sessionWriter, 0)

	handler := protosessionsv3.Handler(
		ctx,
		protosessionsv3.NewDeduplicatingBatchedIOBackend(boltBackend),
		protosessionsv3.NewGenericStorageTimingWheelBackend("default", kv),
		closer,
		requeuer,
		settingsRegistry,
		protosessionsv3.RewriteIDAndUpdateInPlaceStrategy,
	)

	// when
	workerErr := handler(map[string]string{}, &hits.HitProcessingTask{
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
	})
	assert.Nil(t, workerErr)

	require.Nil(t,
		protosessionsv3.SendPingWithTime(
			handler,
			newHitAfter(32*time.Second).ServerReceivedTime.Add(120*time.Second),
		),
	)

	// then - wait for timing wheel to close sessions and write to warehouse
	assert.Eventually(t, func() bool {
		return warehouseDriver.WriteCallCount > 0
	}, 5*time.Second, 100*time.Millisecond, "expected warehouse write")
}

var now = time.Now()

func newHitAfter(d time.Duration) *hits.Hit {
	h := hits.New()
	h.PropertyID = "1337"
	h.ServerReceivedTime = now.Add(d)
	return h
}
