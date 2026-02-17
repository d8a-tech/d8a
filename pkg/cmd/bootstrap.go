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

// bootstrapResult holds resources that need cleanup after command execution.
type bootstrapResult struct {
	cleanup func(context.Context)
}

// bootstrap initializes common resources for production commands (server, receiver, worker).
// It sets up metrics, telemetry, and signal handling for graceful shutdown.
func bootstrap(
	ctx context.Context,
	cancel context.CancelFunc,
	commandName string,
	cmd *cli.Command,
) (*bootstrapResult, error) {
	// Setup metrics
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

	// Start telemetry
	startTelemetry(commandName, cmd.String(telemetryURLFlag.Name))

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logrus.Info("Received shutdown signal, initiating graceful shutdown...")
		cancel()
	}()

	// Return cleanup function for deferred shutdown
	cleanup := func(ctx context.Context) {
		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 5*time.Second)
		defer shutdownCancel()
		if err := metricsSetup.Shutdown(shutdownCtx); err != nil {
			logrus.Errorf("Error shutting down metrics: %v", err)
		}
	}

	return &bootstrapResult{cleanup: cleanup}, nil
}
