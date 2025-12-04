package storage

import (
	"context"
	"errors"
	"time"

	"github.com/d8a-tech/d8a/pkg/monitoring"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	// ErrNilKey is returned when a nil key is provided.
	ErrNilKey = errors.New("key cannot be nil")
	// ErrNilValue is returned when a nil value is provided.
	ErrNilValue = errors.New("value cannot be nil")
	// ErrEmptyKey is returned when an empty key is provided.
	ErrEmptyKey = errors.New("key cannot be empty")
)

type monitoringSet struct {
	Set
	instanceID            string
	operationLatencyHisto metric.Float64Histogram
}

func (m *monitoringSet) Add(key, value []byte) error {
	start := time.Now()
	err := m.Set.Add(key, value)
	duration := time.Since(start).Seconds()
	m.operationLatencyHisto.Record(context.TODO(), duration,
		monitoring.WithAttributes(
			attribute.String("instance_id", m.instanceID),
			attribute.String("operation", "add"),
		))
	return err
}

func (m *monitoringSet) Delete(key, value []byte) error {
	start := time.Now()
	err := m.Set.Delete(key, value)
	duration := time.Since(start).Seconds()
	m.operationLatencyHisto.Record(context.TODO(), duration,
		monitoring.WithAttributes(
			attribute.String("instance_id", m.instanceID),
			attribute.String("operation", "delete"),
		))
	return err
}

func (m *monitoringSet) All(key []byte) ([][]byte, error) {
	start := time.Now()
	values, err := m.Set.All(key)
	duration := time.Since(start).Seconds()
	m.operationLatencyHisto.Record(context.TODO(), duration,
		monitoring.WithAttributes(
			attribute.String("instance_id", m.instanceID),
			attribute.String("operation", "all"),
		))
	return values, err
}

func (m *monitoringSet) Drop(key []byte) error {
	start := time.Now()
	err := m.Set.Drop(key)
	duration := time.Since(start).Seconds()
	m.operationLatencyHisto.Record(context.TODO(), duration,
		monitoring.WithAttributes(
			attribute.String("instance_id", m.instanceID),
			attribute.String("operation", "drop"),
		))
	return err
}

// NewMonitoringSet wraps a Set with monitoring.
func NewMonitoringSet(set Set) Set {
	meter := otel.GetMeterProvider().Meter("storage")
	histogram, _ := meter.Float64Histogram( //nolint:forbidigo // histogram config, not WithAttributes
		"storage.set.operation.latency",
		metric.WithDescription("Latency of Set operations in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.UsBuckets...),
	)
	return &monitoringSet{
		Set:                   set,
		instanceID:            uuid.New().String(),
		operationLatencyHisto: histogram,
	}
}

type monitoringKV struct {
	KV
	instanceID            string
	operationLatencyHisto metric.Float64Histogram
}

func (k *monitoringKV) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		k.operationLatencyHisto.Record(context.TODO(), time.Since(start).Seconds(),
			monitoring.WithAttributes(
				attribute.String("instance_id", k.instanceID),
				attribute.String("operation", "get"),
			))
	}()
	return k.KV.Get(key)
}

func (k *monitoringKV) Set(key, value []byte, opts ...SetOptionsFunc) ([]byte, error) {
	start := time.Now()
	defer func() {
		k.operationLatencyHisto.Record(context.TODO(), time.Since(start).Seconds(),
			monitoring.WithAttributes(
				attribute.String("instance_id", k.instanceID),
				attribute.String("operation", "set"),
			))
	}()
	return k.KV.Set(key, value, opts...)
}

func (k *monitoringKV) Delete(key []byte) error {
	start := time.Now()
	defer func() {
		k.operationLatencyHisto.Record(context.TODO(), time.Since(start).Seconds(),
			monitoring.WithAttributes(
				attribute.String("instance_id", k.instanceID),
				attribute.String("operation", "delete"),
			))
	}()
	return k.KV.Delete(key)
}

// NewMonitoringKV wraps a KV with monitoring.
func NewMonitoringKV(kv KV) KV {
	meter := otel.GetMeterProvider().Meter("storage")
	histogram, _ := meter.Float64Histogram( //nolint:forbidigo // histogram config, not WithAttributes
		"storage.kv.operation.latency",
		metric.WithDescription("Latency of KV operations in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(monitoring.UsBuckets...),
	)
	return &monitoringKV{
		KV:                    kv,
		instanceID:            uuid.New().String(),
		operationLatencyHisto: histogram,
	}
}
