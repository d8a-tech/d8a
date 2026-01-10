// Package sessioncolumns provides column implementations for session data tracking.
package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// TotalEventsColumn is the column for the total events of a session
var TotalEventsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalEvents.ID,
	columns.CoreInterfaces.SessionTotalEvents.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		return len(session.Events), nil
	},
	columns.WithSessionColumnDocs(
		"Session Total Events",
		"The total number of events that occurred during this session. Includes all event types (page views, clicks, custom events, etc.).", // nolint:lll // it's a description
	),
)
