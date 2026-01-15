package ga4

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sseTimeOnPageColumn = columns.NewTimeOnPageColumn(
	columns.CoreInterfaces.SSETimeOnPage.ID,
	columns.CoreInterfaces.SSETimeOnPage.Field,
	columns.TransitionAdvanceWhenEventNameIs(PageViewEventType),
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventName.ID,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Time On Page",
		"Time spent on a particular page, calculated as the interval between subsequent page view events in seconds, or using other events timestamps if no subsequent page view was recorded.", // nolint:lll // it's a description
	),
)
