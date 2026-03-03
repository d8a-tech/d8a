package matomo

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
	return eventNameStr == pageViewEventType
}

var sessionClickIDGclidColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionClickIDGclid.ID,
	columns.CoreInterfaces.SessionClickIDGclid.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventClickIDGclid.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventClickIDGclid.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Click ID GCLID",
		"The Google Click ID (gclid) from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionClickIDDclidColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionClickIDDclid.ID,
	columns.CoreInterfaces.SessionClickIDDclid.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventClickIDDclid.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventClickIDDclid.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Click ID DCLID",
		"The Google Display & Video 360 Click ID (dclid) from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionClickIDGbraidColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionClickIDGbraid.ID,
	columns.CoreInterfaces.SessionClickIDGbraid.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventClickIDGbraid.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventClickIDGbraid.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Click ID GBRAID",
		"The Google Click ID for iOS app-to-web conversions (gbraid) from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionClickIDSrsltidColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionClickIDSrsltid.ID,
	columns.CoreInterfaces.SessionClickIDSrsltid.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventClickIDSrsltid.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventClickIDSrsltid.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Click ID SRSLTID",
		"The Google Shopping Result Click ID (srsltid) from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionClickIDWbraidColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionClickIDWbraid.ID,
	columns.CoreInterfaces.SessionClickIDWbraid.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventClickIDWbraid.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventClickIDWbraid.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Click ID WBRAID",
		"The Google Click ID for iOS web-to-app conversions (wbraid) from the first page view event in the session.", // nolint:lll // it's a description
	),
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
