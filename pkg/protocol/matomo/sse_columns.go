package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sseTimeOnPageColumn = columns.NewTimeOnPageColumn(
	columns.CoreInterfaces.SSETimeOnPage.ID,
	columns.CoreInterfaces.SSETimeOnPage.Field,
	columns.TransitionAdvanceWhenEventNameIs(pageViewEventType),
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventName.ID,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Time On Page",
		"Time spent on a particular page, calculated as the interval between subsequent page view events in seconds, or using other events timestamps if no subsequent page view was recorded.", //nolint:lll // it's a description
	),
)

var sseIsEntryPageColumn = columns.NewFirstLastMatchingEventColumn(
	columns.CoreInterfaces.SSEIsEntryPage.ID,
	columns.CoreInterfaces.SSEIsEntryPage.Field,
	columns.TransitionAdvanceWhenEventNameIs(pageViewEventType),
	true,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventName.ID,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Session Is Entry Page",
		"An integer flag indicating whether this event is the first page view in the session. "+
			"Returns 1 for the first page view event in the session, 0 for all other events. "+
			"Returns 0 if there are no page views in the session.",
	),
)

var sseIsExitPageColumn = columns.NewFirstLastMatchingEventColumn(
	columns.CoreInterfaces.SSEIsExitPage.ID,
	columns.CoreInterfaces.SSEIsExitPage.Field,
	columns.TransitionAdvanceWhenEventNameIs(pageViewEventType),
	false,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventName.ID,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Session Is Exit Page",
		"An integer flag indicating whether this event is the last page view in the session. "+
			"Returns 1 for the last page view event in the session, 0 for all other events. "+
			"Returns 0 if there are no page views in the session.",
	),
)
