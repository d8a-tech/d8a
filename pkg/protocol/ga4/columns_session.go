package ga4

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

func isPageViewEvent(event *schema.Event) bool {
	eventName, ok := event.Values[columns.CoreInterfaces.EventName.Field.Name]
	if !ok {
		return false
	}
	eventNameStr, ok := eventName.(string)
	if !ok {
		return false
	}
	return eventNameStr == PageViewEventType
}

var sessionEntryPageLocationColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionEntryPageLocation.ID,
	columns.CoreInterfaces.SessionEntryPageLocation.Field,
	0,
	columns.CoreInterfaces.EventPageLocation.Field.Name,
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Entry Page Location",
		"The URL of the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionExitPageLocationColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionExitPageLocation.ID,
	columns.CoreInterfaces.SessionExitPageLocation.Field,
	-1,
	columns.CoreInterfaces.EventPageLocation.Field.Name,
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Exit Page Location",
		"The URL of the last page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionEntryPageTitleColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionEntryPageTitle.ID,
	columns.CoreInterfaces.SessionEntryPageTitle.Field,
	0,
	columns.CoreInterfaces.EventPageTitle.Field.Name,
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Entry Page Title",
		"The title of the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionExitPageTitleColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionExitPageTitle.ID,
	columns.CoreInterfaces.SessionExitPageTitle.Field,
	-1,
	columns.CoreInterfaces.EventPageTitle.Field.Name,
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Exit Page Title",
		"The title of the last page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionSecondPageLocationColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionSecondPageLocation.ID,
	columns.CoreInterfaces.SessionSecondPageLocation.Field,
	1,
	columns.CoreInterfaces.EventPageLocation.Field.Name,
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Second Page Location",
		"The URL of the second page view event in the session. Useful for analyzing user navigation patterns after landing.", // nolint:lll // it's a description
	),
)

var sessionSecondPageTitleColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionSecondPageTitle.ID,
	columns.CoreInterfaces.SessionSecondPageTitle.Field,
	1,
	columns.CoreInterfaces.EventPageTitle.Field.Name,
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Second Page Title",
		"The title of the second page view event in the session. Useful for analyzing user navigation patterns after landing.", // nolint:lll // it's a description
	),
)
