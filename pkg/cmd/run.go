// Package cmd provides command line interface for tracker-api
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
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

func configureLogging(ctx context.Context, cmd *cli.Command) (context.Context, error) {
	switch {
	case cmd.Bool(traceFlag.Name):
		logrus.SetLevel(logrus.TraceLevel)
	case cmd.Bool(debugFlag.Name):
		logrus.SetLevel(logrus.DebugLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}
	logrus.Infof("d8a.tech (version %s)", version)
	return ctx, nil
}

func configDocsFlags() []cli.Flag {
	return append([]cli.Flag{airgappedFlag}, getServerFlags()...)
}

func applyModeOverridesBefore(ctx context.Context, cmd *cli.Command) (context.Context, error) {
	updatedCtx, err := applyAirgappedOverridesBefore(ctx, cmd)
	if err != nil {
		return updatedCtx, err
	}

	return applyDeliveryModeOverridesBefore(updatedCtx, cmd)
}

func Run(ctx context.Context, cancel context.CancelFunc, args []string) error { // nolint:funlen,gocognit,gocyclo,lll // it's an entrypoint
	currentRunArgs = append([]string(nil), args...)
	defer func() {
		currentRunArgs = nil
	}()

	app := &cli.Command{
		Name:  "d8a",
		Usage: "d8a.tech - warehouse-native analytics",
		Action: func(context.Context, *cli.Command) error {
			return nil
		},
		Flags: []cli.Flag{
			debugFlag,
			traceFlag,
			configFlag,
			airgappedFlag,
			deliveryModeFlag,
		},
		Version: version,
		Before:  configureLogging,
		Commands: []*cli.Command{
			{
				Name:   "columns",
				Usage:  "Display all the columns for given property ID",
				Before: applyModeOverridesBefore,
				Flags: mergeFlags(
					[]cli.Flag{
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
					getServerFlags(),
				),
				Action: func(_ context.Context, cmd *cli.Command) error {
					if ctx == nil {
						ctx = context.Background()
					}
					converter, cleanup, err := buildCurrencyConverter(cmd)
					if err != nil {
						return err
					}
					converter.Run(ctx)
					defer cleanup()

					geoProvider, geoCleanup, err := buildDBIPProvider(cmd)
					if err != nil {
						return err
					}
					geoProvider.Run(ctx)
					defer geoCleanup()

					protocol := protocolByID(cmd.String(protocolFlag.Name), cmd, converter)
					if protocol == nil {
						return fmt.Errorf("protocol %s not found", cmd.String(protocolFlag.Name))
					}
					cr := columnsRegistry(cmd, converter, geoProvider) // nolint:contextcheck // false positive
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
				Name:   "server",
				Usage:  "Start D8A server. Full configuration reference: https://docs.d8a.tech/articles/config",
				Before: applyModeOverridesBefore,
				Flags:  getServerFlags(),
				Action: func(_ context.Context, cmd *cli.Command) error {
					if err := validateHAFlags("server", cmd); err != nil {
						return err
					}

					if ctx == nil {
						// Context can be set by the caller, create a new one if not set
						ctx, cancel = context.WithCancel(context.Background())
						defer cancel()
					}

					converter, cleanup, err := buildCurrencyConverter(cmd)
					if err != nil {
						return err
					}
					converter.Run(ctx)
					defer cleanup()

					geoProvider, geoCleanup, err := buildDBIPProvider(cmd)
					if err != nil {
						return err
					}
					geoProvider.Run(ctx)
					defer geoCleanup()

					whr := warehouseRegistry(ctx, cmd)
					defer func() {
						if err := whr.Close(); err != nil {
							logrus.WithError(err).Error("failed to close warehouse registry")
						}
					}()

					if err := migrate(ctx, cmd, cmd.String(propertyIDFlag.Name), whr, converter, geoProvider); err != nil {
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

					serverStorage, cleanupReceiverStorage, err := buildReceiverStorage(ctx, cmd, queue.Publisher)
					if err != nil {
						return err
					}
					defer cleanupReceiverStorage()
					runtime, err := buildWorkerRuntime(ctx, cmd, serverStorage, whr, converter, geoProvider)
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
								logrus.Debug("context cancelled, skipping task processing")
								return nil
							default:
								return runtime.Worker.Process(task)
							}
						})
						workerErrChan <- consumeErr
					}()

					// Start server and handle its error
					server := buildReceiverServer(cmd, serverStorage, converter)
					serverErr := server.Run(ctx)
					if serverErr != nil {
						logrus.Errorf("server error: %v", serverErr)
						cancel()
					}

					<-workerErrChan
					return serverErr
				},
			},
			{
				Name:   "receiver",
				Usage:  "Start receiver-only mode (HTTP server; publishes to queue)",
				Before: applyModeOverridesBefore,
				Flags:  getServerFlags(),
				Action: func(_ context.Context, cmd *cli.Command) error {
					if err := validateHAFlags("receiver", cmd); err != nil {
						return err
					}
					if ctx == nil {
						ctx, cancel = context.WithCancel(context.Background())
						defer cancel()
					}

					converter, cleanup, err := buildCurrencyConverter(cmd)
					if err != nil {
						return err
					}
					converter.Run(ctx)
					defer cleanup()

					geoProvider, geoCleanup, err := buildDBIPProvider(cmd)
					if err != nil {
						return err
					}
					geoProvider.Run(ctx)
					defer geoCleanup()

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

					serverStorage, cleanupReceiverStorage, err := buildReceiverStorage(ctx, cmd, queue.Publisher)
					if err != nil {
						return err
					}
					defer cleanupReceiverStorage()
					server := buildReceiverServer(cmd, serverStorage, converter)
					return server.Run(ctx)
				},
			},
			{
				Name:   "worker",
				Usage:  "Start worker-only mode (consumes from queue; no HTTP server)",
				Before: applyModeOverridesBefore,
				Flags:  getServerFlags(),
				Action: func(_ context.Context, cmd *cli.Command) error {
					if err := validateHAFlags("worker", cmd); err != nil {
						return err
					}

					if ctx == nil {
						ctx, cancel = context.WithCancel(context.Background())
						defer cancel()
					}

					converter, cleanup, err := buildCurrencyConverter(cmd)
					if err != nil {
						return err
					}
					converter.Run(ctx)
					defer cleanup()

					geoProvider, geoCleanup, err := buildDBIPProvider(cmd)
					if err != nil {
						return err
					}
					geoProvider.Run(ctx)
					defer geoCleanup()

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

					serverStorage, cleanupReceiverStorage, err := buildReceiverStorage(ctx, cmd, queue.Publisher)
					if err != nil {
						return err
					}
					defer cleanupReceiverStorage()
					whr := warehouseRegistry(ctx, cmd)
					defer func() {
						if err := whr.Close(); err != nil {
							logrus.WithError(err).Error("failed to close warehouse registry")
						}
					}()

					runtime, err := buildWorkerRuntime(ctx, cmd, serverStorage, whr, converter, geoProvider)
					if err != nil {
						return err
					}
					defer runtime.Cleanup()

					return queue.Consumer.Consume(func(task *worker.Task) error {
						select {
						case <-ctx.Done():
							logrus.Debug("context cancelled, skipping task processing")
							return nil
						default:
							return runtime.Worker.Process(task)
						}
					})
				},
			},
			{
				Name:   "migrate",
				Usage:  "Migrate given property to the new schema",
				Before: applyModeOverridesBefore,
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
				Action: func(actionCtx context.Context, cmd *cli.Command) error {
					converter, cleanup, err := buildCurrencyConverter(cmd)
					if err != nil {
						return err
					}
					converter.Run(actionCtx)
					defer cleanup()

					geoProvider, geoCleanup, err := buildDBIPProvider(cmd)
					if err != nil {
						return err
					}
					geoProvider.Run(actionCtx)
					defer geoCleanup()

					whr := warehouseRegistry(actionCtx, cmd)
					defer func() {
						if err := whr.Close(); err != nil {
							logrus.WithError(err).Error("failed to close warehouse registry")
						}
					}()
					return migrate(actionCtx, cmd, cmd.String("property-id"), whr, converter, geoProvider)
				},
			},
			{
				Name:  "config-docs",
				Usage: "Generate configuration documentation from flags",
				Action: func(_ context.Context, cmd *cli.Command) error {
					output, err := generateConfigDocs(configDocsFlags())
					if err != nil {
						return fmt.Errorf("failed to generate config docs: %w", err)
					}
					fmt.Print(output)
					return nil
				},
			},
		},
	}

	app.Commands = append(app.Commands, localfetchCommands()...)

	if err := app.Run(ctx, append([]string{os.Args[0]}, args...)); err != nil {
		return err
	}

	return nil
}

// buildReceiverServer constructs a receiver.Server from CLI flags and the given storage.
// For as long as we don't support multi-property, we return a single protocol.
func buildReceiverServer(cmd *cli.Command, storage receiver.Storage, converter currency.Converter) *receiver.Server {
	currentProtocol := protocolByID(cmd.String(protocolFlag.Name), cmd, converter)
	if currentProtocol == nil {
		logrus.Panicf("protocol %s not found", cmd.String(protocolFlag.Name))
	}

	return receiver.NewServer(
		storage,
		receiver.NewNoopRawLogStorage(),
		receiver.HitValidatingRuleSet(
			1024*util.SafeIntToUint32(cmd.Int(receiverMaxHitKbytesFlag.Name)),
			propertySettings(cmd),
		),
		[]protocol.Protocol{currentProtocol},
		cmd.Int(serverPortFlag.Name),
		receiver.WithHost(cmd.String(serverHostFlag.Name)),
	)
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
					var filtersConfig properties.FiltersConfig
					// Config file is optional; stat before parsing
					if _, err := os.Stat(configFile); err == nil {
						var parseErr error
						filtersConfig, parseErr = properties.ParseFilterConfig(configFile)
						if parseErr != nil {
							logrus.Panicf("failed to parse filters config: %v", parseErr)
						}
					}
					// Override fields from YAML with flag value (flag takes precedence)
					filtersConfig.Fields = cmd.StringSlice(filtersFieldsFlag.Name)

					// Parse and append JSON-encoded conditions from flag/env
					flagConditions := cmd.StringSlice(filtersConditionsFlag.Name)
					for _, conditionJSON := range flagConditions {
						var condition properties.ConditionConfig
						if err := json.Unmarshal([]byte(conditionJSON), &condition); err != nil {
							logrus.Warnf("skipping invalid JSON condition %q: %v", conditionJSON, err)
							continue
						}
						filtersConfig.Conditions = append(filtersConfig.Conditions, condition)
					}

					return &filtersConfig
				}(),
				CustomColumns: func() []properties.CustomColumnConfig {
					customColumns, loadErr := loadProtocolCustomColumns(cmd)
					if loadErr != nil {
						logrus.Panicf("failed to load protocol custom columns config: %v", loadErr)
					}

					return customColumns
				}(),
			},
		),
	)
}
