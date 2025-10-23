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
	columns.WithEventColumnDocs(
		"Client ID",
		"The Client ID is a unique, randomly generated identifier for the device/browser combination. It's stored client-side and is transferred with each event. It's used to distinguish between new/returning visitors.", // nolint:lll // it's a description
	),
)
