package sessioncolumns

import (
	"fmt"
	"time"

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
		serverReceivedTime, err := time.Parse(time.RFC3339, session.Events[0].BoundHit.ServerReceivedTime)
		if err != nil {
			return nil, columns.NewBrokenSessionError(
				fmt.Sprintf("failed to parse server received time: %v", err),
			)
		}
		return serverReceivedTime.Unix(), nil
	})
