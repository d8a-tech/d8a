package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SSETrafficType is a session-scoped event column that reads event metadata
// set by the filter system during testing mode (when active: false).
var SSETrafficType = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSETrafficType.ID,
	columns.CoreInterfaces.SSETrafficType.Field,
	func(s *schema.Session, i int) (any, schema.D8AColumnWriteError) {
		if i >= len(s.Events) {
			return nil, nil //nolint:nilnil // nil is a valid value for this column
		}
		v, ok := s.Events[i].Metadata["engaged_filter_name"]
		if !ok {
			return nil, nil //nolint:nilnil // nil is a valid value for this column
		}
		trafficType, ok := v.(string)
		if !ok {
			return nil, nil //nolint:nilnil // nil is a valid value for this column
		}
		return trafficType, nil
	},
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDocs(
		"Traffic Type",
		"The traffic type classification set by the filter system during testing mode. When a filter condition is set to testing mode (active: false), matching events get this metadata set to the condition name.", // nolint:lll // it's a description
	),
)
