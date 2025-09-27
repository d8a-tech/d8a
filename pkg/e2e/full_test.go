// Package e2e provides end-to-end testing functionality for the tracker-api
package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/bolt"
	"github.com/d8a-tech/d8a/pkg/columns/sessioncolumns"
	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/pings"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/protosessions"
	"github.com/d8a-tech/d8a/pkg/publishers"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/sessions"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/storagepublisher"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	bbolt "go.etcd.io/bbolt"
)

type runningServer struct {
	port           int
	sessionStampKV storage.KV
	compactorKV    storage.KV
	logs           *LogCapture
}

func TestE2EWroteToWarehouse(t *testing.T) {
	withRunningServer(t, func(runningServer runningServer) {
		striker := NewGA4RequestGenerator("localhost", runningServer.port)

		if err := striker.Replay([]HitSequenceItem{
			{
				ClientID:     "client-1",
				EventType:    "page_view",
				SessionStamp: "127.0.0.1",
				Description:  "client 1",
				SleepBefore:  0,
			},
			{
				ClientID:     "client-2",
				EventType:    "scroll",
				SessionStamp: "127.0.0.2",
				Description:  "client 2",
				SleepBefore:  time.Millisecond * 100,
			},
			{
				ClientID:     "client-3",
				EventType:    "page_view",
				SessionStamp: "127.0.0.1",
				Description:  "client 3 (should be same session as client 1)",
				SleepBefore:  time.Millisecond * 100,
			},
		}); err != nil {
			t.Fatalf("Failed to replay GA4 sequence: %v", err)
		}

		require.True(
			t,
			runningServer.logs.waitFor("writing `2` records to `events`", 10*time.Second),
			"expected first proto session closed with 2 events`",
		)
		require.True(
			t,
			runningServer.logs.waitFor("writing `1` records to `events`", 1*time.Second),
			"expected second proto session closed with 1 event",
		)
	})
}

func withRunningServer(t *testing.T, f func(runningServer)) {
	// Create log capture hook
	logCapture := NewLogCapture()
	logrus.AddHook(logCapture)
	defer func() {
		// Clean up hook after test
		for i, hook := range logrus.StandardLogger().Hooks[logrus.InfoLevel] {
			if hook == logCapture {
				logrus.StandardLogger().Hooks[logrus.InfoLevel] = append(
					logrus.StandardLogger().Hooks[logrus.InfoLevel][:i],
					logrus.StandardLogger().Hooks[logrus.InfoLevel][i+1:]...)
				break
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	runningServer := runningServer{
		port:           17031,
		sessionStampKV: storage.NewInMemoryKV(),
		compactorKV:    storage.NewInMemoryKV(),
		logs:           logCapture,
	}

	dbPath := createTempFile(t)
	boltDB, err := bbolt.Open(dbPath, 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt db: %v", err)
	}
	t.Cleanup(func() { _ = boltDB.Close() })
	if err := bolt.EnsureDatabase(boltDB); err != nil {
		t.Fatalf("failed to ensure database: %v", err)
	}
	publisher := publishers.NewPingingPublisher(
		ctx,
		bolt.NewPublisher(boltDB, worker.NewBinaryMessageFormat()),
		time.Millisecond*10,
		pings.NewProcessHitsPingTask(encoding.JSONEncoder),
	)
	serverStorage := storagepublisher.NewAdapter(encoding.JSONEncoder, publisher)
	partitionedStorage := receiver.NewBatchingStorage(
		serverStorage,
		100,
		time.Millisecond*100,
	)

	workerErrChan := make(chan error, 1)

	go func() {
		defer cancel()
		consumer := bolt.NewConsumer(
			ctx,
			boltDB,
			worker.NewBinaryMessageFormat(),
		)
		kv := storage.NewInMemoryKV()
		set := storage.NewInMemorySet()
		w := worker.NewWorker(
			[]worker.TaskHandler{
				worker.NewGenericTaskHandler(
					hits.HitProcessingTaskName,
					encoding.JSONDecoder,
					protosessions.Handler(
						ctx,
						set,
						kv,
						encoding.JSONEncoder,
						encoding.JSONDecoder,
						[]protosessions.Middleware{
							protosessions.NewEvicterMiddleware(runningServer.sessionStampKV, partitionedStorage),
							protosessions.NewCloseTriggerMiddleware(
								storage.NewMonitoringKV(kv),
								storage.NewMonitoringSet(set),
								time.Second*2,
								time.Second*1,
								sessions.NewDirectCloser(
									sessions.NewSessionWriter(
										ctx,
										func() warehouse.Registry {
											return warehouse.NewStaticDriverRegistry(
												warehouse.NewConsoleDriver(),
											)
										}(),
										func() schema.ColumnsRegistry {
											return schema.NewStaticColumnsRegistry(
												map[string]schema.Columns{},
												schema.NewColumns(
													[]schema.SessionColumn{
														sessioncolumns.TotalEventsColumn,
													},
													[]schema.EventColumn{},
												),
											)
										}(),
										schema.NewStaticLayoutRegistry(
											map[string]schema.Layout{},
											schema.NewEmbeddedSessionColumnsLayout(
												"events",
												"sessions_",
											),
										),
									),
									0,
								),
							),
							protosessions.NewCompactorMiddleware(
								runningServer.compactorKV,
								encoding.JSONEncoder,
								encoding.JSONDecoder,
								uint32(4*1024),
							),
						},
					),
				),
			},
			[]worker.Middleware{},
		)

		if err := consumer.Consume(func(task *worker.Task) error {
			// Check if context is done before processing task
			select {
			case <-ctx.Done():
				logrus.Debug("Context cancelled, skipping task processing")
				return nil
			default:
				return w.Process(task)
			}
		}); err != nil {
			workerErrChan <- err
			return
		}

		workerErrChan <- nil
	}()
	serverErrChan := make(chan error, 1)
	go func() {
		serverErr := receiver.Serve(
			ctx,
			partitionedStorage,
			receiver.NewDummyRawLogStorage(),
			runningServer.port,
			protocol.PathProtocolMapping{
				"/g/collect": ga4.NewGA4Protocol(),
			},
			map[string]func(fctx *fasthttp.RequestCtx){},
		)
		serverErrChan <- serverErr
	}()
	// Give the server a chance to start
	time.Sleep(time.Millisecond * 10)

	f(runningServer)
	cancel()
	// Wait for worker to finish if server exits normally
	<-workerErrChan
	<-serverErrChan
}
