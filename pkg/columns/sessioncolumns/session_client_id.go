package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SessionClientIDColumn is the column for the client ID of a session
var SessionClientIDColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionClientID.ID,
	columns.CoreInterfaces.SessionClientID.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		if len(session.Events) == 0 {
			return nil, schema.NewBrokenSessionError("session has no events")
		}
		return string(session.Events[0].BoundHit.ClientID), nil
	},
	columns.WithSessionColumnDocs(
		"Session Client ID",
		"The client ID of the first event in the session, used for session-level reporting.",
	),
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventClientID.ID,
		},
	),
)
