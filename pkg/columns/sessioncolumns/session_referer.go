package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// RefererColumn is the column for the referer of a whole session
var RefererColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionReferer.ID,
	columns.CoreInterfaces.SessionReferer.Field,
	func(session *schema.Session) (any, error) {
		if len(session.Events) == 0 {
			return nil, nil // nolint:nilnil // no referer for empty session
		}
		firstEventReferrer, ok := session.Events[0].Values[columns.CoreInterfaces.EventPageReferrer.Field.Name]
		if !ok {
			return nil, nil // nolint:nilnil // no referer for first event
		}
		return firstEventReferrer, nil
	},
	columns.WithSessionColumnDocs(
		"Session Referer",
		"The referer of the session. Collected from the first event in the session.",
	),
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageReferrer.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
)
