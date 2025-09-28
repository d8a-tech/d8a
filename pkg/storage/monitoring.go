package storage

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	kvOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tracker_api_kv_operation_latency_seconds",
			Help:    "Latency of KV operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"instance_id", "operation"},
	)

	// ErrNilKey is returned when a nil key is provided.
	ErrNilKey = errors.New("key cannot be nil")
	// ErrNilValue is returned when a nil value is provided.
	ErrNilValue = errors.New("value cannot be nil")
	// ErrEmptyKey is returned when an empty key is provided.
	ErrEmptyKey = errors.New("key cannot be empty")
)

func init() {
	prometheus.MustRegister(kvOperationLatency)
}

var (
	setOperationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tracker_api_set_operation_latency_seconds",
			Help:    "Latency of Set operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"instance_id", "operation"},
	)
)

func init() {
	prometheus.MustRegister(setOperationLatency)
}

type monitoringSet struct {
	Set
	instanceID string
}

func (m *monitoringSet) Add(key, value []byte) error {
	start := time.Now()
	err := m.Set.Add(key, value)
	duration := time.Since(start).Seconds()
	setOperationLatency.WithLabelValues(m.instanceID, "add").Observe(duration)
	return err
}

func (m *monitoringSet) All(key []byte) ([][]byte, error) {
	start := time.Now()
	values, err := m.Set.All(key)
	duration := time.Since(start).Seconds()
	setOperationLatency.WithLabelValues(m.instanceID, "all").Observe(duration)
	return values, err
}

func (m *monitoringSet) Delete(key []byte) error {
	start := time.Now()
	err := m.Set.Delete(key)
	duration := time.Since(start).Seconds()
	setOperationLatency.WithLabelValues(m.instanceID, "delete").Observe(duration)
	return err
}

// NewMonitoringSet wraps a Set with Prometheus monitoring.
func NewMonitoringSet(set Set) Set {
	return &monitoringSet{
		Set:        set,
		instanceID: uuid.New().String(),
	}
}

type monitoringKV struct {
	KV
	instanceID string
}

func (k *monitoringKV) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		kvOperationLatency.WithLabelValues(k.instanceID, "get").Observe(time.Since(start).Seconds())
	}()
	return k.KV.Get(key)
}

func (k *monitoringKV) Set(key, value []byte, opts ...SetOptionsFunc) ([]byte, error) {
	start := time.Now()
	defer func() {
		kvOperationLatency.WithLabelValues(k.instanceID, "set").Observe(time.Since(start).Seconds())
	}()
	return k.KV.Set(key, value, opts...)
}

func (k *monitoringKV) Delete(key []byte) error {
	start := time.Now()
	defer func() {
		kvOperationLatency.WithLabelValues(k.instanceID, "delete").Observe(time.Since(start).Seconds())
	}()
	return k.KV.Delete(key)
}

// NewMonitoringKV wraps a KV with Prometheus monitoring.
func NewMonitoringKV(kv KV) KV {
	return &monitoringKV{
		KV:         kv,
		instanceID: uuid.New().String(),
	}
}
