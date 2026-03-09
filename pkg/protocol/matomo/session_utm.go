package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sessionUtmCampaignColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmCampaign.ID,
	columns.CoreInterfaces.SessionUtmCampaign.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmCampaign.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventUtmCampaign.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Campaign",
		"The UTM campaign from the first page view event in the session.", //nolint:lll // it's a description
	),
)

var sessionUtmSourceColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmSource.ID,
	columns.CoreInterfaces.SessionUtmSource.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmSource.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventUtmSource.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Source",
		"The UTM source from the first page view event in the session.", //nolint:lll // it's a description
	),
)

var sessionUtmMediumColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmMedium.ID,
	columns.CoreInterfaces.SessionUtmMedium.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmMedium.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventUtmMedium.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Medium",
		"The UTM medium from the first page view event in the session.", //nolint:lll // it's a description
	),
)

var sessionUtmContentColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmContent.ID,
	columns.CoreInterfaces.SessionUtmContent.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmContent.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventUtmContent.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Content",
		"The UTM content from the first page view event in the session.", //nolint:lll // it's a description
	),
)

var sessionUtmTermColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmTerm.ID,
	columns.CoreInterfaces.SessionUtmTerm.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmTerm.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventUtmTerm.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Term",
		"The UTM term from the first page view event in the session.", //nolint:lll // it's a description
	),
)

var sessionUtmIDColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmID.ID,
	columns.CoreInterfaces.SessionUtmID.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmID.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventUtmID.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session UTM ID",
		"The UTM ID from the first page view event in the session.", //nolint:lll // it's a description
	),
)

var sessionUtmSourcePlatformColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmSourcePlatform.ID,
	columns.CoreInterfaces.SessionUtmSourcePlatform.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmSourcePlatform.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventUtmSourcePlatform.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Source Platform",
		"The UTM source platform from the first page view event in the session.", //nolint:lll // it's a description
	),
)

var sessionUtmCreativeFormatColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmCreativeFormat.ID,
	columns.CoreInterfaces.SessionUtmCreativeFormat.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmCreativeFormat.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventUtmCreativeFormat.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Creative Format",
		"The UTM creative format from the first page view event in the session.", //nolint:lll // it's a description
	),
)

var sessionUtmMarketingTacticColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmMarketingTactic.ID,
	columns.CoreInterfaces.SessionUtmMarketingTactic.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmMarketingTactic.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventUtmMarketingTactic.ID},
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Marketing Tactic",
		"The UTM marketing tactic from the first page view event in the session.", //nolint:lll // it's a description
	),
)
