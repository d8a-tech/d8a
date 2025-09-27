package publishers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/d8a-tech/d8a/pkg/worker"
)

type dropToStdoutPublisher struct{}

// Push implements the Publish interface by writing hits to stdout in JSON format
func (p *dropToStdoutPublisher) Publish(task *worker.Task) error {
	headersJSON, err := json.Marshal(task.Headers)
	if err != nil {
		return err
	}

	logEntry := map[string]any{
		"timestamp": time.Now().Format(time.RFC3339),
		"level":     "warning",
		"task_type": task.Type,
		"headers":   string(headersJSON),
		"purpose":   "recovery",
		"body":      base64.StdEncoding.EncodeToString(task.Body),
	}

	logJSON, err := json.Marshal(logEntry)
	if err != nil {
		return err
	}

	fmt.Println(string(logJSON))
	return nil
}

// NewDropToStdoutPublisher creates a new publisher instance that writes hits to stdout
func NewDropToStdoutPublisher() worker.Publisher {
	return &dropToStdoutPublisher{}
}
