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
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageLocation.Version,
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
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageLocation.Version,
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
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageTitle.Version,
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
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageTitle.Version,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Next Page Title",
		"The title of the next page viewed in the session after the current page. "+
			"Only populated when a page transition is detected. "+
			"Returns nil for the first page or when no page change has occurred.",
	),
)
