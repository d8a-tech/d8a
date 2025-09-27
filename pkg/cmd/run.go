// Package cmd provides command line interface for tracker-api
package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/d8a-tech/d8a/pkg/bolt"
	"github.com/d8a-tech/d8a/pkg/columnset"
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
	"github.com/urfave/cli/v2"
	"github.com/valyala/fasthttp"
	"go.etcd.io/bbolt"
)

var debugFlag *cli.BoolFlag = &cli.BoolFlag{
	Name:    "debug",
	Usage:   "Enable debug mode",
	EnvVars: []string{"DEBUG"},
	Value:   false,
}

var serverPortFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "server-port",
	Usage:   "Port to listen on for HTTP server",
	EnvVars: []string{"SERVER_PORT"},
	Value:   8080,
}

var batcherBatchSizeFlag *cli.IntFlag = &cli.IntFlag{
	Name:    "batcher-batch-size",
	Usage:   "Batch size for the batcher",
	EnvVars: []string{"BATCHER_BATCH_SIZE"},
	Value:   5000,
}

var batcherBatchTimeoutFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "batcher-batch-timeout",
	Usage:   "Batch timeout for the batcher",
	EnvVars: []string{"BATCHER_BATCH_TIMEOUT"},
	Value:   5 * time.Second,
}

var closerSessionDurationFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "closer-session-duration",
	Usage:   "Session duration for the closer",
	EnvVars: []string{"CLOSER_SESSION_DURATION"},
	Value:   1 * time.Minute,
}

var closerTickIntervalFlag *cli.DurationFlag = &cli.DurationFlag{
	Name:    "closer-tick-interval",
	Usage:   "Tick interval for the closer",
	EnvVars: []string{"CLOSER_TICK_INTERVAL"},
	Value:   1 * time.Second,
}

func mergeFlags(allFlags ...[]cli.Flag) []cli.Flag {
	var endFlags []cli.Flag
	for _, singleFlagCollection := range allFlags {
		endFlags = append(endFlags, singleFlagCollection...)
	}
	return endFlags
}

// Run starts the tracker-api server
func Run(ctx context.Context, cancel context.CancelFunc, args []string) { // nolint:funlen,gocognit,lll // it's an entrypoint
	app := &cli.App{
		Name:  "d8a",
		Usage: "D8a.tech - GA4-compatible analytics platform",
		Action: func(*cli.Context) error {
			return nil
		},
		Flags: []cli.Flag{
			debugFlag,
		},
		Before: func(c *cli.Context) error {
			if c.Bool(debugFlag.Name) {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},
		Commands: []*cli.Command{
			{
				Name:  "server",
				Usage: "Start d8a demo server",
				Flags: mergeFlags(
					[]cli.Flag{
						serverPortFlag,
						batcherBatchSizeFlag,
						batcherBatchTimeoutFlag,
						closerSessionDurationFlag,
						closerTickIntervalFlag,
					},
				),
				Action: func(c *cli.Context) error {
					if ctx == nil {
						// Context can be set by the caller, create a new one if not set
						ctx, cancel = context.WithCancel(context.Background())
						defer cancel()
					}

					if err := migrate(ctx, c, "1337"); err != nil {
						return fmt.Errorf("failed to migrate: %w", err)
					}

					// Set up signal handling
					sigChan := make(chan os.Signal, 1)
					signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
					go func() {
						<-sigChan
						logrus.Info("Received shutdown signal, initiating graceful shutdown...")
						cancel()
					}()

					boltDB, err := bbolt.Open("/tmp/bolt.db", 0o600, nil)
					if err != nil {
						logrus.Fatalf("failed to open bolt db: %v", err)
					}
					defer boltDB.Close()
					if err := bolt.EnsureDatabase(boltDB); err != nil {
						logrus.Fatalf("failed to ensure database: %v", err)
					}
					boltPublisher := publishers.NewPingingPublisher(
						ctx,
						bolt.NewPublisher(boltDB, worker.NewBinaryMessageFormat()),
						time.Second*1,
						pings.NewProcessHitsPingTask(encoding.ZlibCBOREncoder),
					)
					serverStorage := storagepublisher.NewStoragePublisherAdapter(encoding.ZlibCBOREncoder, boltPublisher)
					workerErrChan := make(chan error, 1)
					go func() {
						defer cancel()
						consumer := bolt.NewConsumer(
							ctx,
							boltDB,
							worker.NewBinaryMessageFormat(),
						)
						kv, err := bolt.NewBoltKV("/tmp/bolt_kv.db")
						if err != nil {
							logrus.Fatalf("failed to create bolt kv: %v", err)
						}
						set, err := bolt.NewBoltSet("/tmp/bolt_set.db")
						if err != nil {
							logrus.Fatalf("failed to create bolt set: %v", err)
						}
						w := worker.NewWorker(
							[]worker.TaskHandler{
								worker.NewGenericTaskHandler(
									hits.HitProcessingTaskName,
									encoding.ZlibCBORDecoder,
									protosessions.Handler(
										ctx,
										set,
										kv,
										encoding.GobEncoder,
										encoding.GobDecoder,
										[]protosessions.Middleware{
											protosessions.NewEvicterMiddleware(storage.NewMonitoringKV(kv), serverStorage),
											protosessions.NewCloseTriggerMiddleware(
												storage.NewMonitoringKV(kv),
												storage.NewMonitoringSet(set),
												c.Duration(closerSessionDurationFlag.Name),
												c.Duration(closerTickIntervalFlag.Name),
												sessions.NewDirectCloser(
													sessions.NewSessionWriter(
														ctx,
														warehouseRegistry(ctx, c),
														columnsRegistry(),
														schema.NewStaticLayoutRegistry(
															map[string]schema.Layout{},
															schema.NewEmbeddedSessionColumnsLayout(
																getTableNames().events,
																getTableNames().sessionsColumnPrefix,
															),
														),
													),
													5*time.Second,
												),
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

					rawLogStorage := receiver.NewFromStorageSetRawLogStorage(
						storage.NewInMemorySet(),
						storage.NewInMemorySet(),
					)

					// Start server and handle its error
					serverErr := receiver.Serve(
						ctx,
						serverStorage,
						receiver.NewBatchingRawlogStorage(
							rawLogStorage,
							c.Int(batcherBatchSizeFlag.Name),
							c.Duration(batcherBatchTimeoutFlag.Name),
						),
						c.Int(serverPortFlag.Name),
						protocol.PathProtocolMapping{
							"/g/collect": ga4.NewGA4Protocol(),
						},
						map[string]func(fctx *fasthttp.RequestCtx){
							"/rawlogs": receiver.RawLogMainPageHandlerFromReader(rawLogStorage),
							"/rawlog/": receiver.RawLogDetailPageHandlerFromReader(rawLogStorage),
						},
					)
					if serverErr != nil {
						logrus.Errorf("Server error: %v", serverErr)
						cancel()
					}

					<-workerErrChan
					return serverErr
				},
			},
			{
				Name:  "migrate",
				Usage: "Migrate given property to the new schema",
				Flags: mergeFlags(
					[]cli.Flag{
						&cli.StringFlag{
							Name:     "property-id",
							Usage:    "Property ID to migrate",
							EnvVars:  []string{"PROPERTY_ID"},
							Required: true,
						},
					},
				),
				Action: func(c *cli.Context) error {
					return migrate(ctx, c, c.String("property-id"))
				},
			},
			createRawlogDebuggerCommand(),
		},
	}

	if err := app.Run(append([]string{os.Args[0]}, args...)); err != nil {
		log.Fatal(err)
	}
}

func columnsRegistry() schema.ColumnsRegistry {
	return columnset.DefaultColumnRegistry(ga4.NewGA4Protocol())
}

func warehouseRegistry(ctx context.Context, c *cli.Context) warehouse.Registry {
	return warehouse.NewStaticDriverRegistry(
		warehouse.NewConsoleDriver(),
	)
}
