// Package eventcolumns provides column implementations for event data tracking.
package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// UserIDColumn is the column for the user ID of an event
var UserIDColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventUserID.ID,
	columns.CoreInterfaces.EventUserID.Field,
	func(event *schema.Event) (any, error) {
		return event.BoundHit.UserID, nil
	},
)
