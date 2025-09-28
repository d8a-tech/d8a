// Package bolt provides BoltDB storage operations for hit tracking.
package bolt

import (
	"errors"
	"strconv"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// TasksPrefix is the prefix for hit-related keys.
const TasksPrefix = "tasks/"

// TasksBucket is the bucket name for hit data.
const TasksBucket = "tasks"

// TasksCurrentNanosecondTs returns a key for the current timestamp.
func TasksCurrentNanosecondTs() string {
	currentTs := time.Now().UnixNano()
	return TasksPrefix + strconv.FormatInt(currentTs, 10)
}

// ParseNano extracts timestamp from a hits key.
func ParseNano(tasksKey string) (int64, error) {
	if !strings.HasPrefix(tasksKey, TasksPrefix) {
		return 0, errors.New("tasksKey must start with TasksPrefix")
	}
	tasksKey = strings.TrimPrefix(tasksKey, TasksPrefix)
	return strconv.ParseInt(tasksKey, 10, 64)
}

// EnsureDatabase creates required buckets if they don't exist.
func EnsureDatabase(db *bolt.DB) error {
	return db.Update(func(txn *bolt.Tx) error {
		_, err := txn.CreateBucketIfNotExists([]byte(TasksBucket))
		return err
	})
}
