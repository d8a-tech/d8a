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

// UsBuckets provides histogram boundaries for microsecond-scale measurements (1us to 100ms) in seconds.
var UsBuckets = []float64{
	0.000001, 0.000002, 0.000005, 0.00001, 0.00002, 0.00005,
	0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1,
}

// MsBuckets provides histogram boundaries for millisecond-scale measurements (500us to 5s) in seconds.
var MsBuckets = []float64{
	0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0,
}

// SBuckets provides histogram boundaries for second-scale measurements (5ms to 2m) in seconds.
var SBuckets = []float64{
	0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0,
}

// WithAttributes is a wrapper around metric.WithAttributes to satisfy linter rules.
func WithAttributes(attrs ...attribute.KeyValue) metric.RecordOption {
	return metric.WithAttributes(attrs...) //nolint:forbidigo // this IS the wrapper
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
func SetupMetrics(
	ctx context.Context,
	enabled bool,
	otelEndpoint, serviceName, serviceVersion string,
) (*MetricsSetup, error) {
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
			sdkmetric.WithInterval(1*time.Second),
		)),
	)
	otel.SetMeterProvider(meterProvider)
	logrus.Infof("OTel metrics configured with endpoint %s (export interval: 1s)", otelEndpoint)
	return &MetricsSetup{meterProvider: meterProvider}, nil
}
