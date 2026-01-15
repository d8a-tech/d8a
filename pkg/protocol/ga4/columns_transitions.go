package ga4

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var eventPreviousPageLocationColumn = columns.NewValueTransitionColumn(
	ProtocolInterfaces.EventPreviousPageLocation.ID,
	ProtocolInterfaces.EventPreviousPageLocation.Field,
	columns.CoreInterfaces.EventPageLocation.Field.Name,
	columns.TransitionAdvanceWhenEventNameIs("page_view"),
	columns.TransitionDirectionBackward,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventPageLocation.ID,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Previous Page Location",
		"The URL of the previous page viewed in the session before the current page. "+
			"Only populated when a page transition is detected. "+
			"Returns nil for the first page or when no page change has occurred.",
	),
)

var eventNextPageLocationColumn = columns.NewValueTransitionColumn(
	ProtocolInterfaces.EventNextPageLocation.ID,
	ProtocolInterfaces.EventNextPageLocation.Field,
	columns.CoreInterfaces.EventPageLocation.Field.Name,
	columns.TransitionAdvanceWhenEventNameIs("page_view"),
	columns.TransitionDirectionForward,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventPageLocation.ID,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Next Page Location",
		"The URL of the next page viewed in the session after the current page. "+
			"Only populated when a page transition is detected. "+
			"Returns nil for the first page or when no page change has occurred.",
	),
)

var eventPreviousPageTitleColumn = columns.NewValueTransitionColumn(
	ProtocolInterfaces.EventPreviousPageTitle.ID,
	ProtocolInterfaces.EventPreviousPageTitle.Field,
	columns.CoreInterfaces.EventPageTitle.Field.Name,
	columns.TransitionAdvanceWhenEventNameIs("page_view"),
	columns.TransitionDirectionBackward,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventPageTitle.ID,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Previous Page Title",
		"The title of the previous page viewed in the session before the current page. "+
			"Only populated when a page transition is detected. "+
			"Returns nil for the first page or when no page change has occurred.",
	),
)

var eventNextPageTitleColumn = columns.NewValueTransitionColumn(
	ProtocolInterfaces.EventNextPageTitle.ID,
	ProtocolInterfaces.EventNextPageTitle.Field,
	columns.CoreInterfaces.EventPageTitle.Field.Name,
	columns.TransitionAdvanceWhenEventNameIs("page_view"),
	columns.TransitionDirectionForward,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.EventPageTitle.ID,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Next Page Title",
		"The title of the next page viewed in the session after the current page. "+
			"Only populated when a page transition is detected. "+
			"Returns nil for the first page or when no page change has occurred.",
	),
)

var sseIsEntryPageColumn = columns.NewFirstLastMatchingEventColumn(
	columns.CoreInterfaces.SSEIsEntryPage.ID,
	columns.CoreInterfaces.SSEIsEntryPage.Field,
	columns.TransitionAdvanceWhenEventNameIs("page_view"),
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
	columns.TransitionAdvanceWhenEventNameIs("page_view"),
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
