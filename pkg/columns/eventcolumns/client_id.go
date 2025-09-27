// Package eventcolumns provides column implementations for event data tracking.
package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// ClientIDColumn is the column for the client ID of an event
var ClientIDColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventClientID.ID,
	columns.CoreInterfaces.EventClientID.Field,
	func(event *schema.Event) (any, error) {
		return event.BoundHit.ClientID, nil
	},
)
