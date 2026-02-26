package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/d8a-tech/d8a/pkg/monitoring"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

type bootstrapResult struct {
	cleanup func(context.Context)
}

func bootstrap(
	ctx context.Context,
	cancel context.CancelFunc,
	commandName string,
	cmd *cli.Command,
) (*bootstrapResult, error) {
	metricsSetup, err := monitoring.SetupMetrics(
		ctx,
		cmd.Bool(monitoringEnabledFlag.Name),
		cmd.String(monitoringOTelEndpointFlag.Name),
		cmd.Duration(monitoringOTelExportIntervalFlag.Name),
		cmd.Bool(monitoringOTelInsecureFlag.Name),
		"d8a",
		"1.0.0",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to setup metrics: %w", err)
	}

	startTelemetry(commandName, cmd.String(telemetryURLFlag.Name))

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logrus.Info("received shutdown signal, initiating graceful shutdown...")
		cancel()
	}()

	cleanup := func(ctx context.Context) {
		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 5*time.Second)
		defer shutdownCancel()
		if err := metricsSetup.Shutdown(shutdownCtx); err != nil {
			logrus.Errorf("error shutting down metrics: %v", err)
		}
	}

	return &bootstrapResult{cleanup: cleanup}, nil
}
