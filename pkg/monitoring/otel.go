package monitoring

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// WithAttributes is a wrapper around metric.WithAttributes to satisfy linter rules.
func WithAttributes(attrs ...attribute.KeyValue) metric.RecordOption {
	return metric.WithAttributes(attrs...)
}

// MetricsSetup encapsulates metrics configuration and lifecycle.
type MetricsSetup struct {
	meterProvider *sdkmetric.MeterProvider
}

// Shutdown gracefully shuts down the metrics provider.
func (m *MetricsSetup) Shutdown(ctx context.Context) error {
	if m.meterProvider != nil {
		return m.meterProvider.Shutdown(ctx)
	}
	return nil
}

// SetupMetrics initializes OTel metrics if enabled.
// If disabled, does nothing - OTel will use its built-in noop provider.
func SetupMetrics(ctx context.Context, enabled bool, otelEndpoint, serviceName, serviceVersion string) (*MetricsSetup, error) {
	if !enabled {
		return &MetricsSetup{meterProvider: nil}, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
			attribute.String("service.version", serviceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(otelEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
			metricExporter,
			sdkmetric.WithInterval(30*time.Second),
		)),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{
				Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: []float64{
						0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500,
						5000, 7500, 10000, 20000, 50000, 100000,
					},
				},
			},
		)),
	)
	otel.SetMeterProvider(meterProvider)
	logrus.Infof("OTel metrics configured with endpoint %s (export interval: 30s)", otelEndpoint)
	return &MetricsSetup{meterProvider: meterProvider}, nil
}
