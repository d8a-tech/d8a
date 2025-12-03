package worker

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type monitoringPublisher struct {
	publisher      Publisher
	bytesPublished metric.Int64Counter
	tasksPublished metric.Int64Counter
}

// Publish implements the Publisher interface.
func (m *monitoringPublisher) Publish(task *Task) error {
	bytesCount := int64(len(task.Body))

	m.bytesPublished.Add(context.Background(), bytesCount,
		metric.WithAttributes(
			attribute.String("task_type", task.Type),
		))
	m.tasksPublished.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("task_type", task.Type),
		))

	return m.publisher.Publish(task)
}

// NewMonitoringPublisher creates a new publisher decorator that tracks metrics.
func NewMonitoringPublisher(publisher Publisher) Publisher {
	meter := otel.GetMeterProvider().Meter("worker")

	bytesPublished, _ := meter.Int64Counter(
		"worker.publisher.bytes_published",
		metric.WithDescription("Total bytes published by publisher"),
		metric.WithUnit("By"),
	)
	tasksPublished, _ := meter.Int64Counter(
		"worker.publisher.tasks_published",
		metric.WithDescription("Total tasks published by publisher"),
	)

	return &monitoringPublisher{
		publisher:      publisher,
		bytesPublished: bytesPublished,
		tasksPublished: tasksPublished,
	}
}
