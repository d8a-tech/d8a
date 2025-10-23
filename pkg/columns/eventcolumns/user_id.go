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
	columns.WithEventColumnDocs(
		"User ID",
		"An optional user-provided identifier for authenticated users. This is set by the tracking implementation when a user is logged in and allows tracking across devices and sessions for the same user.", // nolint:lll // it's a description
	),
)
