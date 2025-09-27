package receiver

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/d8a-tech/d8a/pkg/hits"
)

type dropToStdoutStorage struct{}

// Push implements the Storage interface by writing hits to stdout in JSON format
func (s *dropToStdoutStorage) Push(hits []*hits.Hit) error {
	for _, hit := range hits {
		// Convert hit to JSON for readable output
		jsonData, err := json.MarshalIndent(hit, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal hit to JSON: %w", err)
		}

		// Write to stdout with a newline for readability
		if _, err := fmt.Fprintf(os.Stdout, "%s\n", jsonData); err != nil {
			return fmt.Errorf("failed to write to stdout: %w", err)
		}
	}
	return nil
}

// NewDropToStdoutStorage creates a new storage instance that writes hits to stdout
func NewDropToStdoutStorage() Storage {
	return &dropToStdoutStorage{}
}
