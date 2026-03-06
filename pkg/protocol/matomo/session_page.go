package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionEntryPageLocationColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionEntryPageLocation.ID,
	columns.CoreInterfaces.SessionEntryPageLocation.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageLocation.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageLocation.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Entry Page Location",
		"The URL of the first page view in the session.",
	),
)

var sessionSecondPageLocationColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionSecondPageLocation.ID,
	columns.CoreInterfaces.SessionSecondPageLocation.Field,
	1,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageLocation.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageLocation.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Second Page Location",
		"The URL of the second page view in the session.",
	),
)

var sessionExitPageLocationColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionExitPageLocation.ID,
	columns.CoreInterfaces.SessionExitPageLocation.Field,
	-1,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageLocation.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageLocation.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Exit Page Location",
		"The URL of the last page view in the session.",
	),
)

var sessionEntryPageTitleColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionEntryPageTitle.ID,
	columns.CoreInterfaces.SessionEntryPageTitle.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageTitle.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageTitle.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Entry Page Title",
		"The title of the first page view in the session.",
	),
)

var sessionSecondPageTitleColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionSecondPageTitle.ID,
	columns.CoreInterfaces.SessionSecondPageTitle.Field,
	1,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageTitle.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageTitle.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Second Page Title",
		"The title of the second page view in the session.",
	),
)

var sessionExitPageTitleColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionExitPageTitle.ID,
	columns.CoreInterfaces.SessionExitPageTitle.Field,
	-1,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageTitle.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageTitle.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Exit Page Title",
		"The title of the last page view in the session.",
	),
)
