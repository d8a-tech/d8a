package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/urfave/cli/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func createPerflabTestCommand() *cli.Command {
	return &cli.Command{
		Name:  "perflab-test",
		Usage: "Send random telemetry data to OTel collector for testing",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "otel-endpoint",
				Usage:   "OTel collector endpoint",
				Sources: cli.EnvVars("OTEL_ENDPOINT"),
				Value:   "localhost:4317",
			},
			&cli.DurationFlag{
				Name:    "duration",
				Usage:   "How long to send data",
				Sources: cli.EnvVars("PERFLAB_DURATION"),
				Value:   30 * time.Second,
			},
			&cli.IntFlag{
				Name:    "trace-interval-ms",
				Usage:   "Interval between traces in milliseconds",
				Sources: cli.EnvVars("PERFLAB_TRACE_INTERVAL_MS"),
				Value:   1000,
			},
			&cli.IntFlag{
				Name:    "metric-interval-ms",
				Usage:   "Interval between metrics in milliseconds",
				Sources: cli.EnvVars("PERFLAB_METRIC_INTERVAL_MS"),
				Value:   500,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			endpoint := cmd.String("otel-endpoint")
			duration := cmd.Duration("duration")
			traceInterval := time.Duration(cmd.Int("trace-interval-ms")) * time.Millisecond
			metricInterval := time.Duration(cmd.Int("metric-interval-ms")) * time.Millisecond

			runCtx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()

			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()

			// Create resource
			res, err := resource.New(ctx,
				resource.WithAttributes(
					attribute.String("service.name", "perflab-test"),
					attribute.String("service.version", "1.0.0"),
				),
			)
			if err != nil {
				return fmt.Errorf("failed to create resource: %w", err)
			}

			// Setup trace exporter
			traceExporter, err := otlptracegrpc.New(ctx,
				otlptracegrpc.WithEndpoint(endpoint),
				otlptracegrpc.WithInsecure(),
			)
			if err != nil {
				return fmt.Errorf("failed to create trace exporter: %w", err)
			}
			defer func() {
				if err := traceExporter.Shutdown(shutdownCtx); err != nil {
					fmt.Printf("Error shutting down trace exporter: %v\n", err)
				}
			}()

			tracerProvider := sdktrace.NewTracerProvider(
				sdktrace.WithBatcher(traceExporter),
				sdktrace.WithResource(res),
			)
			otel.SetTracerProvider(tracerProvider)
			defer func() {
				if err := tracerProvider.Shutdown(shutdownCtx); err != nil {
					fmt.Printf("Error shutting down tracer provider: %v\n", err)
				}
			}()

			// Setup metric exporter
			metricExporter, err := otlpmetricgrpc.New(ctx,
				otlpmetricgrpc.WithEndpoint(endpoint),
				otlpmetricgrpc.WithInsecure(),
			)
			if err != nil {
				return fmt.Errorf("failed to create metric exporter: %w", err)
			}
			defer func() {
				if err := metricExporter.Shutdown(shutdownCtx); err != nil {
					fmt.Printf("Error shutting down metric exporter: %v\n", err)
				}
			}()

			meterProvider := sdkmetric.NewMeterProvider(
				sdkmetric.WithResource(res),
				sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
			)
			otel.SetMeterProvider(meterProvider)
			defer func() {
				if err := meterProvider.Shutdown(shutdownCtx); err != nil {
					fmt.Printf("Error shutting down meter provider: %v\n", err)
				}
			}()

			// Create meter and counters
			meter := meterProvider.Meter("perflab-test")
			counter, err := meter.Int64Counter(
				"perflab.requests",
				metric.WithDescription("Number of requests"),
			)
			if err != nil {
				return fmt.Errorf("failed to create counter: %w", err)
			}

			histogram, err := meter.Float64Histogram(
				"perflab.request.duration",
				metric.WithDescription("Request duration in seconds"),
				metric.WithUnit("s"),
			)
			if err != nil {
				return fmt.Errorf("failed to create histogram: %w", err)
			}

			// Create tracer
			tracer := tracerProvider.Tracer("perflab-test")

			// Start sending traces and metrics
			traceTicker := time.NewTicker(traceInterval)
			defer traceTicker.Stop()

			metricTicker := time.NewTicker(metricInterval)
			defer metricTicker.Stop()

			fmt.Printf("Sending telemetry data to %s for %v...\n", endpoint, duration)
			fmt.Printf("Trace interval: %v, Metric interval: %v\n", traceInterval, metricInterval)

			rand.Seed(time.Now().UnixNano())
			operations := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
			statuses := []string{"200", "404", "500", "503"}

			for {
				select {
				case <-runCtx.Done():
					fmt.Println("Finished sending telemetry data")
					return nil
				case <-traceTicker.C:
					// Send a trace
					operation := operations[rand.Intn(len(operations))]
					status := statuses[rand.Intn(len(statuses))]
					spanDuration := time.Duration(rand.Intn(1000)) * time.Millisecond

					_, span := tracer.Start(runCtx, operation,
						trace.WithAttributes(
							attribute.String("http.method", operation),
							attribute.String("http.status_code", status),
							attribute.String("http.route", "/api/v1/"+operation),
						),
					)
					time.Sleep(spanDuration)
					span.End()
					fmt.Printf("Sent trace: %s %s (duration: %v)\n", operation, status, spanDuration)

				case <-metricTicker.C:
					// Send metrics
					metricDuration := rand.Float64() * 2.0

					counter.Add(runCtx, 1,
						metric.WithAttributes(
							attribute.String("method", operations[rand.Intn(len(operations))]),
							attribute.String("status", statuses[rand.Intn(len(statuses))]),
						),
					)

					histogram.Record(runCtx, metricDuration,
						metric.WithAttributes(
							attribute.String("method", operations[rand.Intn(len(operations))]),
						),
					)
					fmt.Printf("Sent metrics: counter=1, histogram=%.2f\n", metricDuration)
				}
			}
		},
	}
}
