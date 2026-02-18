// Package cmd provides command line interface for tracker-api
package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/telemetry"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

func mergeFlags(allFlags ...[]cli.Flag) []cli.Flag {
	totalLen := 0
	for _, singleFlagCollection := range allFlags {
		totalLen += len(singleFlagCollection)
	}
	endFlags := make([]cli.Flag, 0, totalLen)
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

func startTelemetry(itemName, telemetryURL string) {
	if telemetryURL == "" {
		return
	}
	telemetry.Start(
		telemetry.WithURL(telemetryURL),
		telemetry.WithEvent(
			telemetry.OnStartup,
			telemetry.SimpleEvent(
				"app_started",
				telemetry.ClientIDGeneratedOnStartup(),
			).WithParam(
				"app_version",
				telemetry.Raw(version),
			).WithParam(
				"item_name",
				telemetry.Raw(itemName),
			),
		),
		telemetry.WithEvent(
			telemetry.EveryXHours(24*time.Hour),
			telemetry.SimpleEvent(
				"app_running",
				telemetry.ClientIDGeneratedOnStartup(),
			).WithParam(
				"app_version", telemetry.Raw(version),
			).WithParam(
				"params_exposure_time", telemetry.NumberOfSecsSinceStarted(),
			).WithParam(
				"item_name",
				telemetry.Raw(itemName),
			),
		),
	)
}

// Run starts the tracker-api server
func Run(ctx context.Context, cancel context.CancelFunc, args []string) { // nolint:funlen,gocognit,gocyclo,lll // it's an entrypoint
	app := &cli.Command{
		Name:  "d8a",
		Usage: "D8a.tech - GA4-compatible analytics platform",
		Action: func(context.Context, *cli.Command) error {
			return nil
		},
		Flags: []cli.Flag{
			debugFlag,
			traceFlag,
			configFlag,
		},
		Version: version,
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			switch {
			case cmd.Bool(traceFlag.Name):
				logrus.SetLevel(logrus.TraceLevel)
			case cmd.Bool(debugFlag.Name):
				logrus.SetLevel(logrus.DebugLevel)
			default:
				logrus.SetLevel(logrus.InfoLevel)
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
						protocolFlag,
					},
				),
				Action: func(_ context.Context, cmd *cli.Command) error {
					protocol := protocolByID(cmd.String(protocolFlag.Name), cmd)
					if protocol == nil {
						return fmt.Errorf("protocol %s not found", cmd.String(protocolFlag.Name))
					}
					cr := columnsRegistry(cmd) // nolint:contextcheck // false positive
					columnData, err := cr.Get(cmd.String("property-id"))
					if err != nil {
						return err
					}
					ordering := schema.NewInterfaceDefinitionOrderKeeper(
						columns.CoreInterfaces,
						protocol.Interfaces(), // This hardcodes ga4, it's fine for now for OSS
					)
					columnData = schema.Sorted(columnData, ordering)
					formatters := map[string]columnsFormatter{
						"console":  newConsoleColumnsFormatter(),
						"json":     newJSONColumnsFormatter(),
						"csv":      newCSVColumnsFormatter(),
						"markdown": newMarkdownColumnsFormatter(),
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
				Usage: "Start D8A server. Full configuration reference: https://docs.d8a.tech/articles/config",
				Flags: getServerFlags(),
				Action: func(_ context.Context, cmd *cli.Command) error {
					if err := validateHAFlags("server", cmd); err != nil {
						return err
					}
					if ctx == nil {
						// Context can be set by the caller, create a new one if not set
						ctx, cancel = context.WithCancel(context.Background())
						defer cancel()
					}

					if err := migrate(ctx, cmd, cmd.String(propertyIDFlag.Name)); err != nil {
						return fmt.Errorf("failed to migrate: %w", err)
					}

					bs, err := bootstrap(ctx, cancel, "server", cmd)
					if err != nil {
						return err
					}
					defer bs.cleanup(context.Background()) //nolint:contextcheck // shutdown needs fresh context

					queue, err := buildQueue(ctx, cmd)
					if err != nil {
						return err
					}
					defer func() {
						if closeErr := queue.Cleanup(); closeErr != nil {
							logrus.Error("failed to cleanup queue:", closeErr)
						}
					}()

					serverStorage := buildReceiverStorage(ctx, cmd, queue.Publisher)
					runtime, err := buildWorkerRuntime(ctx, cmd, serverStorage)
					if err != nil {
						return err
					}
					defer runtime.Cleanup()

					workerErrChan := make(chan error, 1)
					go func() {
						defer cancel()
						consumeErr := queue.Consumer.Consume(func(task *worker.Task) error {
							// Check if context is done before processing task
							select {
							case <-ctx.Done():
								logrus.Debug("Context cancelled, skipping task processing")
								return nil
							default:
								return runtime.Worker.Process(task)
							}
						})
						workerErrChan <- consumeErr
					}()

					// Start server and handle its error
					server := receiver.NewServer(
						serverStorage,
						receiver.NewNoopRawLogStorage(),
						receiver.HitValidatingRuleSet(
							1024*util.SafeIntToUint32(cmd.Int(receiverMaxHitKbytesFlag.Name)),
							propertySettings(cmd),
						),
						// For as long as we don't support multi-property, we return a single protocol.
						func() []protocol.Protocol {
							currentProtocol := protocolByID(cmd.String(protocolFlag.Name), cmd)
							if currentProtocol == nil {
								logrus.Panicf("protocol %s not found", cmd.String(protocolFlag.Name))
							}
							return []protocol.Protocol{currentProtocol}
						}(),
						cmd.Int(serverPortFlag.Name),
					)
					serverErr := server.Run(ctx)
					if serverErr != nil {
						logrus.Errorf("Server error: %v", serverErr)
						cancel()
					}

					<-workerErrChan
					return serverErr
				},
			},
			{
				Name:  "receiver",
				Usage: "Start receiver-only mode (HTTP server; publishes to queue)",
				Flags: getServerFlags(),
				Action: func(_ context.Context, cmd *cli.Command) error {
					if err := validateHAFlags("receiver", cmd); err != nil {
						return err
					}
					if ctx == nil {
						ctx, cancel = context.WithCancel(context.Background())
						defer cancel()
					}

					bs, err := bootstrap(ctx, cancel, "receiver", cmd)
					if err != nil {
						return err
					}
					defer bs.cleanup(context.Background()) //nolint:contextcheck // shutdown needs fresh context

					queue, err := buildQueue(ctx, cmd)
					if err != nil {
						return err
					}
					defer func() {
						if closeErr := queue.Cleanup(); closeErr != nil {
							logrus.Error("failed to cleanup queue:", closeErr)
						}
					}()

					serverStorage := buildReceiverStorage(ctx, cmd, queue.Publisher)
					server := receiver.NewServer(
						serverStorage,
						receiver.NewNoopRawLogStorage(),
						receiver.HitValidatingRuleSet(
							1024*util.SafeIntToUint32(cmd.Int(receiverMaxHitKbytesFlag.Name)),
							propertySettings(cmd),
						),
						func() []protocol.Protocol {
							currentProtocol := protocolByID(cmd.String(protocolFlag.Name), cmd)
							if currentProtocol == nil {
								logrus.Panicf("protocol %s not found", cmd.String(protocolFlag.Name))
							}
							return []protocol.Protocol{currentProtocol}
						}(),
						cmd.Int(serverPortFlag.Name),
					)
					return server.Run(ctx)
				},
			},
			{
				Name:  "worker",
				Usage: "Start worker-only mode (consumes from queue; no HTTP server)",
				Flags: getServerFlags(),
				Action: func(_ context.Context, cmd *cli.Command) error {
					if err := validateHAFlags("worker", cmd); err != nil {
						return err
					}
					if ctx == nil {
						ctx, cancel = context.WithCancel(context.Background())
						defer cancel()
					}

					bs, err := bootstrap(ctx, cancel, "worker", cmd)
					if err != nil {
						return err
					}
					defer bs.cleanup(context.Background()) //nolint:contextcheck // shutdown needs fresh context

					queue, err := buildQueue(ctx, cmd)
					if err != nil {
						return err
					}
					defer func() {
						if closeErr := queue.Cleanup(); closeErr != nil {
							logrus.Error("failed to cleanup queue:", closeErr)
						}
					}()

					serverStorage := buildReceiverStorage(ctx, cmd, queue.Publisher)
					runtime, err := buildWorkerRuntime(ctx, cmd, serverStorage)
					if err != nil {
						return err
					}
					defer runtime.Cleanup()

					return queue.Consumer.Consume(func(task *worker.Task) error {
						select {
						case <-ctx.Done():
							logrus.Debug("Context cancelled, skipping task processing")
							return nil
						default:
							return runtime.Worker.Process(task)
						}
					})
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
						protocolFlag,
					},
					warehouseConfigFlags,
				),
				Action: func(_ context.Context, cmd *cli.Command) error {
					return migrate(ctx, cmd, cmd.String("property-id"))
				},
			},
			{
				Name:  "config-docs",
				Usage: "Generate configuration documentation from flags",
				Action: func(_ context.Context, cmd *cli.Command) error {
					flags := getServerFlags()
					output, err := generateConfigDocs(flags)
					if err != nil {
						return fmt.Errorf("failed to generate config docs: %w", err)
					}
					fmt.Print(output)
					return nil
				},
			},
		},
	}

	if err := app.Run(ctx, append([]string{os.Args[0]}, args...)); err != nil {
		log.Fatal(err)
	}
}

func propertySettings(cmd *cli.Command) properties.SettingsRegistry {
	return properties.NewStaticSettingsRegistry(
		[]properties.Settings{},
		properties.WithDefaultConfig(
			&properties.Settings{
				ProtocolID:                 cmd.String(protocolFlag.Name),
				PropertyID:                 cmd.String(propertyIDFlag.Name),
				PropertyName:               cmd.String(propertyNameFlag.Name),
				PropertyMeasurementID:      "-",
				SplitByUserID:              cmd.Bool(propertySettingsSplitByUserIDFlag.Name),
				SplitByCampaign:            cmd.Bool(propertySettingsSplitByCampaignFlag.Name),
				SplitByTimeSinceFirstEvent: cmd.Duration(propertySettingsSplitByTimeSinceFirstEventFlag.Name),
				SplitByMaxEvents:           cmd.Int(propertySettingsSplitByMaxEventsFlag.Name),

				SessionTimeout:            cmd.Duration(sessionsTimeoutFlag.Name),
				SessionJoinBySessionStamp: cmd.Bool(sessionsJoinBySessionStampFlag.Name),
				SessionJoinByUserID:       cmd.Bool(sessionsJoinByUserIDFlag.Name),
				Filters: func() *properties.FiltersConfig {
					filtersConfig, err := properties.ParseFilterConfig(configFile)
					if err != nil {
						logrus.Panicf("failed to parse filters config: %v", err)
					}
					// Override fields from YAML with flag value (flag takes precedence)
					filtersConfig.Fields = cmd.StringSlice(filtersFieldsFlag.Name)
					return &filtersConfig
				}(),
			},
		),
	)
}
