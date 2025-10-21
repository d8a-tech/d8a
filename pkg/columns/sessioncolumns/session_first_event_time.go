package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// FirstEventTimeColumn is the column for the first event time of a session
var FirstEventTimeColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionFirstEventTime.ID,
	columns.CoreInterfaces.SessionFirstEventTime.Field,
	func(session *schema.Session) (any, error) {
		if len(session.Events) == 0 {
			return nil, columns.NewBrokenSessionError("session has no events")
		}
		return session.Events[0].BoundHit.ServerReceivedTime.Unix(), nil
	})
