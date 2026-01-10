package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SSEIsEntry is a session-scoped event column that writes whether the event is the first event in the session
var SSEIsEntry = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSEIsEntry.ID,
	columns.CoreInterfaces.SSEIsEntry.Field,
	func(s *schema.Session, i int) (any, schema.D8AColumnWriteError) {
		if len(s.Events) == 0 {
			return nil, nil // nolint:nilnil // nil is valid for this column
		}
		if i == 0 {
			return int64(1), nil
		}
		return int64(0), nil
	},
	columns.WithSessionScopedEventColumnDocs(
		"Session Is Entry Event",
		"An integer flag indicating whether this event is the first event (entry point) of the session. Returns 1 for the first event in the session, 0 for all subsequent events.", // nolint:lll // it's a description
	),
)
