// Package eventcolumns provides column implementations for event data tracking.
package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// EventIDColumn is the column for the event ID of an event
var EventNameColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventName.ID,
	columns.CoreInterfaces.EventName.Field,
	func(event *schema.Event) (any, error) {
		if event.BoundHit.EventName == "" {
			return nil, columns.NewBrokenEventError("event name is empty")
		}
		return event.BoundHit.EventName, nil
	},
	columns.WithEventColumnDocs(
		"Event Name",
		"The name of the event. This identifies the action the user performed (e.g., 'page_view', 'click', 'purchase', 'sign_up').", // nolint:lll // it's a description
	),
)
