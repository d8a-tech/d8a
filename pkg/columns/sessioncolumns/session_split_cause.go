package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/splitter"
	"github.com/sirupsen/logrus"
)

// SplitCauseColumn is the column for the split cause of a session
var SplitCauseColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionSplitCause.ID,
	columns.CoreInterfaces.SessionSplitCause.Field,
	func(session *schema.Session) (any, error) {
		if len(session.Events) == 0 {
			return nil, nil //nolint:nilnil // nil is a valid value for this column
		}
		v, ok := session.Events[0].Metadata["session_split_cause"]
		if !ok {
			return nil, nil //nolint:nilnil // nil is a valid value for this column
		}
		splitCause, ok := v.(splitter.SplitCause)
		if !ok {
			logrus.Warnf("session split cause is not a splitter.SplitCause: %v", v)
			return nil, nil //nolint:nilnil // nil is a valid value for this column
		}
		return string(splitCause), nil
	},
	columns.WithSessionColumnDocs(
		"Session Split Cause",
		"The cause of the split of the session. If the session was not split, this will be null.",
	),
)
