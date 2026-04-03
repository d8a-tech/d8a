package spools

import (
	"context"
	"time"

	"github.com/d8a-tech/d8a/pkg/monitoring"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	spoolMeter = otel.GetMeterProvider().Meter("spools")

	appendLatencyHistogram, _ = spoolMeter.Float64Histogram( //nolint:forbidigo // histogram config, not WithAttributes
		"spools.append.latency",
		metric.WithDescription("Latency of successful spool append operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	appendPayloadBytesHistogram, _ = spoolMeter.Int64Histogram( //nolint:forbidigo // histogram config, not WithAttributes
		"spools.append.payload_bytes",
		metric.WithDescription("Payload size in bytes for successful spool appends"),
		metric.WithUnit("By"),
	)
	flushReturnedBatchCounter, _ = spoolMeter.Int64Counter(
		"spools.flush.returned_batch.count",
		metric.WithDescription("Number of non-empty batches returned by spool next()"),
	)
	flushBatchBytesHistogram, _ = spoolMeter.Int64Histogram( //nolint:forbidigo // histogram config, not WithAttributes
		"spools.flush.returned_batch.bytes",
		metric.WithDescription("Total payload bytes in each non-empty returned batch"),
		metric.WithUnit("By"),
	)
	flushBatchProcessingLatencyHistogram, _ = spoolMeter.Float64Histogram( //nolint:forbidigo // instrument setup
		"spools.flush.batch_processing.latency",
		metric.WithDescription("Elapsed wall time between consecutive non-empty next() returns"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	flushKeyProcessingLatencyHistogram, _ = spoolMeter.Float64Histogram( //nolint:forbidigo // instrument setup
		"spools.flush.key_processing.latency",
		metric.WithDescription("Total time spent processing all inflight files for a key"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.SBuckets...),
	)
)

func initMonitoringInstruments() {
	spoolMeter = otel.GetMeterProvider().Meter("spools")

	appendLatencyHistogram, _ = spoolMeter.Float64Histogram( //nolint:forbidigo // histogram config, not WithAttributes
		"spools.append.latency",
		metric.WithDescription("Latency of successful spool append operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
	)
	appendPayloadBytesHistogram, _ = spoolMeter.Int64Histogram( //nolint:forbidigo // histogram config, not WithAttributes
		"spools.append.payload_bytes",
		metric.WithDescription("Payload size in bytes for successful spool appends"),
		metric.WithUnit("By"),
	)
	flushReturnedBatchCounter, _ = spoolMeter.Int64Counter(
		"spools.flush.returned_batch.count",
		metric.WithDescription("Number of non-empty batches returned by spool next()"),
	)
	flushBatchBytesHistogram, _ = spoolMeter.Int64Histogram( //nolint:forbidigo // histogram config, not WithAttributes
		"spools.flush.returned_batch.bytes",
		metric.WithDescription("Total payload bytes in each non-empty returned batch"),
		metric.WithUnit("By"),
	)
	flushBatchProcessingLatencyHistogram, _ =
		spoolMeter.Float64Histogram( //nolint:forbidigo // instrument setup
			"spools.flush.batch_processing.latency",
			metric.WithDescription("Elapsed wall time between consecutive non-empty next() returns"),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(monitoring.MsBuckets...),
		)
	flushKeyProcessingLatencyHistogram, _ =
		spoolMeter.Float64Histogram( //nolint:forbidigo // instrument setup
			"spools.flush.key_processing.latency",
			metric.WithDescription("Total time spent processing all inflight files for a key"),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(monitoring.SBuckets...),
		)
}

func init() {
	initMonitoringInstruments()
}

func recordAppendMetrics(dir string, payloadBytes int, duration time.Duration) {
	attrs := monitoring.WithAttributes(attribute.String("dir", dir))
	appendLatencyHistogram.Record(context.TODO(), duration.Seconds(), attrs)
	appendPayloadBytesHistogram.Record(context.TODO(), int64(payloadBytes), attrs)
}

func recordFlushReturnedBatchMetrics(dir string, payloadBytes int64) {
	flushReturnedBatchCounter.Add(
		context.TODO(),
		1,
		metric.WithAttributes(attribute.String("dir", dir)),
	)
	flushBatchBytesHistogram.Record(
		context.TODO(),
		payloadBytes,
		monitoring.WithAttributes(attribute.String("dir", dir)),
	)
}

func recordFlushBatchProcessingLatency(dir string, duration time.Duration) {
	flushBatchProcessingLatencyHistogram.Record(
		context.TODO(),
		duration.Seconds(),
		monitoring.WithAttributes(attribute.String("dir", dir)),
	)
}

func recordFlushKeyProcessingLatency(dir string, duration time.Duration) {
	flushKeyProcessingLatencyHistogram.Record(
		context.TODO(),
		duration.Seconds(),
		monitoring.WithAttributes(attribute.String("dir", dir)),
	)
}
