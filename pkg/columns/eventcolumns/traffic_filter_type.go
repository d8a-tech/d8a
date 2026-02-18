package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SSETrafficFilterName is a session-scoped event column that reads event metadata
// set by the filter system during testing mode (when test_mode: true).
var SSETrafficFilterName = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSETrafficFilterName.ID,
	columns.CoreInterfaces.SSETrafficFilterName.Field,
	func(s *schema.Session, i int) (any, schema.D8AColumnWriteError) {
		if i >= len(s.Events) {
			return nil, nil //nolint:nilnil // nil is a valid value for this column
		}
		v, ok := s.Events[i].Metadata["traffic_filter_name"]
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
		"Name of the traffic filter that matched this event in testing mode. If the filter were active, this event would have been excluded.", // nolint:lll // it's a description
	),
)
