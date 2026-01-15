package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// ReferrerColumn is the column for the referrer of a whole session
var ReferrerColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionReferrer.ID,
	columns.CoreInterfaces.SessionReferrer.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		if len(session.Events) == 0 {
			return nil, nil // nolint:nilnil // no referrer for empty session
		}
		firstEventReferrer, ok := session.Events[0].Values[columns.CoreInterfaces.EventPageReferrer.Field.Name]
		if !ok {
			return nil, nil // nolint:nilnil // no referrer for first event
		}
		return firstEventReferrer, nil
	},
	columns.WithSessionColumnDocs(
		"Session Referrer",
		"The referrer of the session. Collected from the first event in the session.",
	),
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventPageReferrer.ID,
		},
	),
)
