// Package eventcolumns provides column implementations for event data tracking.
package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// EventIDColumn is the column for the event ID of an event
var EventIDColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventID.ID,
	columns.CoreInterfaces.EventID.Field,
	func(event *schema.Event) (any, error) {
		return event.BoundHit.ID, nil
	},
	columns.WithEventColumnDocs(
		"Event ID",
		"A unique identifier for this specific event. Generated server-side when the hit is received. Used to deduplicate events and track individual occurrences.", // nolint:lll // it's a description
	),
)
