package e2e

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// LogCapture captures logrus entries for testing
type LogCapture struct {
	mu      sync.RWMutex
	entries []*logrus.Entry
}

// NewLogCapture creates a new log capture hook
func NewLogCapture() *LogCapture {
	return &LogCapture{
		entries: make([]*logrus.Entry, 0),
	}
}

// Levels returns all log levels to capture
func (hook *LogCapture) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire captures the log entry
func (hook *LogCapture) Fire(entry *logrus.Entry) error {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	hook.entries = append(hook.entries, entry)
	return nil
}

// GetEntries returns all captured entries
func (hook *LogCapture) GetEntries() []*logrus.Entry {
	hook.mu.RLock()
	defer hook.mu.RUnlock()
	entriesCopy := make([]*logrus.Entry, len(hook.entries))
	copy(entriesCopy, hook.entries)
	return entriesCopy
}

// HasMessage checks if any captured entry contains the given message
func (hook *LogCapture) HasMessage(message string) bool {
	hook.mu.RLock()
	defer hook.mu.RUnlock()
	for _, entry := range hook.entries {
		if strings.Contains(entry.Message, message) {
			return true
		}
	}
	return false
}

// waitFor waits for a specific message to appear in logs within timeout
func (hook *LogCapture) waitFor(message string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if hook.HasMessage(message) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func createTempFile(t *testing.T) string {
	f, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			logrus.Error(err)
		}
	}()
	return f.Name()
}
