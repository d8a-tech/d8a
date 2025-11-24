package sessioncolumns

import (
	"errors"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SessionIDColumn is the column for the session ID
var SessionIDColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionID.ID,
	columns.CoreInterfaces.SessionID.Field,
	func(session *schema.Session) (any, error) {
		if len(session.Events) == 0 {
			return nil, fmt.Errorf("session has no events")
		}
		firstEventID, ok := session.Events[0].Values[columns.CoreInterfaces.EventID.Field.Name]
		if !ok {
			return nil, errors.New("first event doesn't have ID")
		}
		return firstEventID, nil
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventID.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session ID",
		"A unique identifier for the session, derived from the first event's ID in the session, used to group all events that belong to the same user session.", // nolint:lll // it's a description
	),
)
