// Package objectstorage provides an object storage-based implementation of worker.Publisher and worker.Consumer
// using Go Cloud Development Kit (CDK) blob storage abstraction.
package objectstorage

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// GenerateTaskKey creates a timestamp-based key for task storage.
// Format: {timestamp_nanos}_{task_id}
// This ensures lexicographic ordering for FIFO processing and collision resistance.
func GenerateTaskKey(taskID string) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%d_%s", timestamp, taskID)
}

func joinPrefix(prefix, key string) string {
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	key = strings.TrimPrefix(key, "/")
	if prefix == "" {
		return key
	}
	return path.Join(prefix, key)
}

// ParseTimestampFromKey extracts the timestamp from a task key.
// Returns the timestamp in nanoseconds and any parsing error.
func ParseTimestampFromKey(key string) (int64, error) {
	// Remove prefix and tasks subdir to get timestamp_uuid part
	parts := strings.Split(key, "/")
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid key format: %s", key)
	}

	timestampPart := parts[len(parts)-1] // Get the last part

	// Split by underscore to separate timestamp from task ID
	timestampUUIDParts := strings.Split(timestampPart, "_")
	if len(timestampUUIDParts) < 2 {
		return 0, fmt.Errorf("invalid key format, missing timestamp_uuid: %s", key)
	}

	return strconv.ParseInt(timestampUUIDParts[0], 10, 64)
}

// GenerateTaskID creates a new UUID for task identification.
func GenerateTaskID() string {
	return uuid.New().String()
}
