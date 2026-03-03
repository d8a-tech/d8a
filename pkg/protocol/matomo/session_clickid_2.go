package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionClickIDFbclidColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionClickIDFbclid.ID,
	columns.CoreInterfaces.SessionClickIDFbclid.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventClickIDFbclid.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventClickIDFbclid.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Click ID FBCLID",
		"The Meta Click ID (fbclid) from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionClickIDMsclkidColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionClickIDMsclkid.ID,
	columns.CoreInterfaces.SessionClickIDMsclkid.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventClickIDMsclkid.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventClickIDMsclkid.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Click ID MSCLKID",
		"The Microsoft Advertising Click ID (msclkid) from the first page view event in the session.", // nolint:lll // it's a description
	),
)
