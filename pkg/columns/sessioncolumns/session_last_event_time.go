package sessioncolumns

import (
	"fmt"
	"time"

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
		serverReceivedTime, err := time.Parse(time.RFC3339, session.Events[len(session.Events)-1].BoundHit.ServerReceivedTime)
		if err != nil {
			return nil, columns.NewBrokenSessionError(
				fmt.Sprintf("failed to parse server received time: %v", err),
			)
		}
		return serverReceivedTime.Unix(), nil
	})
