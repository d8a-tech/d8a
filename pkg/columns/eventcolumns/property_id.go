package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// PropertyIDColumn is the column for the property ID of an event
var PropertyIDColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventPropertyID.ID,
	columns.CoreInterfaces.EventPropertyID.Field,
	func(event *schema.Event) (any, error) {
		return event.BoundHit.PropertyID, nil
	},
	columns.WithEventColumnDocs(
		"Property ID",
		"The unique identifier for the property (website or app) that sent this event. Used to distinguish between different tracked properties and route data to appropriate destinations.", // nolint:lll // it's a description
	),
)
