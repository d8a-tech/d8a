package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/properties"
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
		"The unique identifier for the property (website or app) that sent this event, used to distinguish between different tracked properties and route data to appropriate destinations.", // nolint:lll // it's a description
	),
)

// PropertyNameColumn is the column for the name of the property of an event
func PropertyNameColumn(propertySource properties.SettingsRegistry) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		columns.CoreInterfaces.EventPropertyName.ID,
		columns.CoreInterfaces.EventPropertyName.Field,
		func(event *schema.Event) (any, error) {
			property, err := propertySource.GetByPropertyID(event.BoundHit.PropertyID)
			if err != nil {
				return "", nil
			}
			return property.PropertyName, nil
		},
	)
}
