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
		return string(event.BoundHit.ClientID), nil
	},
	columns.WithEventColumnDocs(
		"Client ID",
		"The Client ID is a unique, randomly generated identifier assigned to each device-browser pair or app installation. It is stored client-side and sent with every event, enabling the analytics system to calculate the number of unique users. It is also used as one of the identifiers that merge events into a session.", // nolint:lll // it's a description
	),
)
