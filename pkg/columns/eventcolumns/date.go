// Package eventcolumns provides column implementations for event data tracking.
package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// DateColumn is the column for the date of an event
var DateColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventDate.ID,
	columns.CoreInterfaces.EventDate.Field,
	func(event *schema.Event) (any, error) {
		return event.BoundHit.Timestamp, nil
	},
)
