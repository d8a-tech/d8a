package ga4

import "github.com/d8a-tech/d8a/pkg/columns"

var sourceManualCampaignIDColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualCampaignID.ID,
	ProtocolInterfaces.EventSourceManualCampaignID.Field,
	"utm_id",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualCampaignID.ID)),
	),
)

var sourceManualCampaignNameColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualCampaignName.ID,
	ProtocolInterfaces.EventSourceManualCampaignName.Field,
	"utm_campaign",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualCampaignName.ID)),
	),
)

var sourceManualSourceColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualSource.ID,
	ProtocolInterfaces.EventSourceManualSource.Field,
	"utm_source",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualSource.ID)),
	),
)

var sourceManualMediumColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualMedium.ID,
	ProtocolInterfaces.EventSourceManualMedium.Field,
	"utm_medium",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualMedium.ID)),
	),
)

var sourceManualTermColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualTerm.ID,
	ProtocolInterfaces.EventSourceManualTerm.Field,
	"utm_term",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualTerm.ID)),
	),
)

var sourceManualContentColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualContent.ID,
	ProtocolInterfaces.EventSourceManualContent.Field,
	"utm_content",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualContent.ID)),
	),
)

var sourceManualSourcePlatformColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualSourcePlatform.ID,
	ProtocolInterfaces.EventSourceManualSourcePlatform.Field,
	"utm_source_platform",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualSourcePlatform.ID)),
	),
)

var sourceManualCreativeFormatColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualCreativeFormat.ID,
	ProtocolInterfaces.EventSourceManualCreativeFormat.Field,
	"utm_creative_format",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualCreativeFormat.ID)),
	),
)

var sourceManualMarketingTacticColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualMarketingTactic.ID,
	ProtocolInterfaces.EventSourceManualMarketingTactic.Field,
	"utm_marketing_tactic",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualMarketingTactic.ID)),
	),
)

var sourceGclidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceGclid.ID,
	ProtocolInterfaces.EventSourceGclid.Field,
	"gclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceGclid.ID)),
	),
)

var sourceDclidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceDclid.ID,
	ProtocolInterfaces.EventSourceDclid.Field,
	"dclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceDclid.ID)),
	),
)

var sourceSrsltidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceSrsltid.ID,
	ProtocolInterfaces.EventSourceSrsltid.Field,
	"srsltid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceSrsltid.ID)),
	),
)
