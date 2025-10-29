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
	"github.com/d8a-tech/d8a/pkg/currency"
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
	"github.com/urfave/cli/v3"
	"github.com/valyala/fasthttp"
	"go.etcd.io/bbolt"
)

func mergeFlags(allFlags ...[]cli.Flag) []cli.Flag {
	var endFlags []cli.Flag
	for _, singleFlagCollection := range allFlags {
		endFlags = append(endFlags, singleFlagCollection...)
	}
	return endFlags
}

var currencyConverter currency.Converter = func() currency.Converter {
	converter, err := currency.NewFWAConverter(nil)
	if err != nil {
		logrus.Fatalf("failed to create currency converter: %v", err)
	}
	return converter
}()

// Run starts the tracker-api server
func Run(ctx context.Context, cancel context.CancelFunc, args []string) { // nolint:funlen,gocognit,lll // it's an entrypoint
	app := &cli.Command{
		Name:  "d8a",
		Usage: "D8a.tech - GA4-compatible analytics platform",
		Action: func(context.Context, *cli.Command) error {
			return nil
		},
		Flags: []cli.Flag{
			debugFlag,
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Bool(debugFlag.Name) {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return ctx, nil
		},
		Commands: []*cli.Command{
			{
				Name:  "columns",
				Usage: "Display all the columns for given property ID",
				Flags: mergeFlags(
					[]cli.Flag{
						&cli.StringFlag{
							Name:     "property-id",
							Usage:    "Property ID to display columns for",
							Sources:  cli.EnvVars("PROPERTY_ID"),
							Required: true,
						},
						&cli.StringFlag{
							Name:     "output",
							Usage:    "Output format",
							Aliases:  []string{"o"},
							Sources:  cli.EnvVars("OUTPUT"),
							Value:    "console",
							Required: false,
						},
					},
				),
				Action: func(_ context.Context, cmd *cli.Command) error {
					cr := columnsRegistry(cmd) // nolint:contextcheck // false positive
					columnData, err := cr.Get(cmd.String("property-id"))
					if err != nil {
						return err
					}
					formatters := map[string]columnsFormatter{
						"console": newConsoleColumnsFormatter(),
						"json":    newJSONColumnsFormatter(),
						"csv":     newCSVColumnsFormatter(),
					}
					formatter, ok := formatters[cmd.String("output")]
					if !ok {
						return fmt.Errorf("invalid output format: %s, possible options are %#v", cmd.String("output"), formatters)
					}
					output, err := formatter.Format(columnData)
					if err != nil {
						return err
					}
					fmt.Println(output)
					return nil
				},
			},
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
						dbipEnabled,
						dbipDestinationDirectory,
						dbipDownloadTimeoutFlag,
					},
				),
				Action: func(_ context.Context, cmd *cli.Command) error {
					if ctx == nil {
						// Context can be set by the caller, create a new one if not set
						ctx, cancel = context.WithCancel(context.Background())
						defer cancel()
					}

					if err := migrate(ctx, cmd, "1337"); err != nil {
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
					defer func() {
						if err := boltDB.Close(); err != nil {
							logrus.Errorf("failed to close bolt db: %v", err)
						}
					}()
					if err := bolt.EnsureDatabase(boltDB); err != nil {
						logrus.Fatalf("failed to ensure database: %v", err)
					}
					boltPublisher := publishers.NewPingingPublisher(
						ctx,
						bolt.NewPublisher(boltDB, worker.NewBinaryMessageFormat()),
						time.Second*1,
						pings.NewProcessHitsPingTask(encoding.ZlibCBOREncoder),
					)
					serverStorage := storagepublisher.NewAdapter(encoding.ZlibCBOREncoder, boltPublisher)
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
												cmd.Duration(closerSessionDurationFlag.Name),
												cmd.Duration(closerTickIntervalFlag.Name),
												sessions.NewDirectCloser(
													sessions.NewSessionWriter(
														ctx,
														warehouseRegistry(ctx, cmd),
														columnsRegistry(cmd), // nolint:contextcheck // false positive
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
							cmd.Int(batcherBatchSizeFlag.Name),
							cmd.Duration(batcherBatchTimeoutFlag.Name),
						),
						cmd.Int(serverPortFlag.Name),
						protocol.PathProtocolMapping{
							"/g/collect": ga4.NewGA4Protocol(currencyConverter),
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
							Sources:  cli.EnvVars("PROPERTY_ID"),
							Required: true,
						},
					},
				),
				Action: func(_ context.Context, cmd *cli.Command) error {
					return migrate(ctx, cmd, cmd.String("property-id"))
				},
			},
			createRawlogDebuggerCommand(),
		},
	}

	if err := app.Run(ctx, append([]string{os.Args[0]}, args...)); err != nil {
		log.Fatal(err)
	}
}

func warehouseRegistry(_ context.Context, _ *cli.Command) warehouse.Registry {
	return warehouse.NewStaticDriverRegistry(
		warehouse.NewConsoleDriver(),
	)
}
