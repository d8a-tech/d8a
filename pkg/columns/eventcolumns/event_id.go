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
)
