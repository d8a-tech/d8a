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
	func(session *schema.Session) (any, error) {
		return len(session.Events), nil
	},
)
