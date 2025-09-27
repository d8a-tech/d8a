package cmd

import (
	"fmt"
	"time"
)

type tables struct {
	events               string
	sessionsColumnPrefix string
}

// generateDescendingSortableID generates an identifier that sorts in descending order
// by subtracting the current timestamp from a fixed timestamp.
// This ensures newer identifiers appear first when sorted ascending.
func generateDescendingSortableID() string {
	// Fixed timestamp far in the future (Unix timestamp for ~2035)
	// This should be larger than any reasonable current timestamp
	fixedTimestamp := int64(2071711709)

	currentTimestamp := time.Now().Unix()
	descendingID := fixedTimestamp - currentTimestamp

	// Format with leading zeros to ensure constant length (10 digits)
	return fmt.Sprintf("%010d", descendingID)
}

var id = generateDescendingSortableID()

// getTableNames returns the table names for the current property.
func getTableNames() tables {
	return tables{
		events:               fmt.Sprintf("events_%s", id),
		sessionsColumnPrefix: "",
	}
}
