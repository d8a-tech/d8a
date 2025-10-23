package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// LastEventTimeColumn is the column for the last event time of a session
var LastEventTimeColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionLastEventTime.ID,
	columns.CoreInterfaces.SessionLastEventTime.Field,
	func(session *schema.Session) (any, error) {
		if len(session.Events) == 0 {
			return nil, columns.NewBrokenSessionError("session has no events")
		}
		return session.Events[len(session.Events)-1].BoundHit.ServerReceivedTime.Unix(), nil
	},
	columns.WithSessionColumnDocs(
		"Session Last Event Time",
		"The timestamp of the last event in the session. Marks the end of the user's session and is used along with first_event_time to calculate session duration.", // nolint:lll // it's a description
	),
)
