package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// TimestampColumn is the column for the timestamp of an event
var TimestampColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventTimestamp.ID,
	columns.CoreInterfaces.EventTimestamp.Field,
	func(event *schema.Event) (any, error) {
		return event.BoundHit.Timestamp, nil
	},
)
