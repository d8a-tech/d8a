package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SSEIsEntry is a session-scoped event column that writes whether the event is the first event in the session
var SSEIsEntry = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSEIsEntry.ID,
	columns.CoreInterfaces.SSEIsEntry.Field,
	func(e *schema.Event, s *schema.Session) (any, error) {
		if len(s.Events) == 0 {
			return nil, nil // nolint:nilnil // nil is valid for this column
		}
		if e == s.Events[0] {
			return true, nil
		}
		return false, nil
	},
	columns.WithSessionScopedEventColumnDocs(
		"Is Entry Event",
		"A boolean flag indicating whether this event is the first event (entry point) of the session. True for the first event in the session, false for all subsequent events.", // nolint:lll // it's a description
	),
)
