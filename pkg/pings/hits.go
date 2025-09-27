// Package pings provides utilities for creating and handling ping tasks.
package pings

import (
	"time"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/worker"
)

// IsPingMetadataKey is the metadata key used to identify ping tasks.
const IsPingMetadataKey = "is-ping"

// IsPingMetadataValue is the metadata value used to mark a task as a ping.
const IsPingMetadataValue = "true"

// PingTimestampMetadataKey is the metadata key for storing ping timestamp.
const PingTimestampMetadataKey = "ping-timestamp"

// NewProcessHitsPingTask creates a new empty process hits task. It may be used in conjunction
// with pinging publisher to make the handler advance processing ticks.
func NewProcessHitsPingTask(
	encoder encoding.EncoderFunc,
) func() (*worker.Task, error) {
	return func() (*worker.Task, error) {
		taskData, err := worker.SerializeTaskData(encoder, hits.HitProcessingTask{
			Hits: []*hits.Hit{},
		})
		if err != nil {
			return nil, err
		}
		return worker.NewTask(hits.HitProcessingTaskName, map[string]string{
			IsPingMetadataKey:        IsPingMetadataValue,
			PingTimestampMetadataKey: time.Now().Format(time.RFC3339),
		}, taskData), nil
	}
}

// IsTaskAPing checks if the task is a ping by checking the metadata.
func IsTaskAPing(taskMetadata map[string]string) (bool, time.Time) {
	isPing, ok := taskMetadata[IsPingMetadataKey]
	if !ok {
		return false, time.Time{}
	}
	if isPing != IsPingMetadataValue {
		return false, time.Time{}
	}
	pingTimestamp, ok := taskMetadata[PingTimestampMetadataKey]
	if !ok {
		return false, time.Time{}
	}
	pingTimestampTime, err := time.Parse(time.RFC3339, pingTimestamp)
	if err != nil {
		return false, time.Time{}
	}
	return true, pingTimestampTime
}
