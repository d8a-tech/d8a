package ga4

import "github.com/d8a-tech/d8a/pkg/columns"

var sourceManualCampaignIDColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualCampaignID.ID,
	ProtocolInterfaces.EventSourceManualCampaignID.Field,
	"utm_id",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualCampaignID.ID)),
	),
	columns.WithEventColumnDocs(
		"Source Manual Campaign ID",
		"Campaign ID from URL parameters. Extracted from 'utm_id' in the page URL. Part of manual traffic source attribution.",
	),
)

var sourceManualCampaignNameColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualCampaignName.ID,
	ProtocolInterfaces.EventSourceManualCampaignName.Field,
	"utm_campaign",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualCampaignName.ID)),
	),
	columns.WithEventColumnDocs(
		"Source Manual Campaign Name",
		"Campaign name from URL parameters. Extracted from 'utm_campaign' in the page URL. Part of manual traffic source attribution.", // nolint:lll // it's a description
	),
)

var sourceManualSourceColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualSource.ID,
	ProtocolInterfaces.EventSourceManualSource.Field,
	"utm_source",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualSource.ID)),
	),
	columns.WithEventColumnDocs(
		"Source Manual Source",
		"Traffic source from URL parameters. Extracted from 'utm_source' in the page URL. Part of manual traffic source attribution.",
	),
)

var sourceManualMediumColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualMedium.ID,
	ProtocolInterfaces.EventSourceManualMedium.Field,
	"utm_medium",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualMedium.ID)),
	),
	columns.WithEventColumnDocs(
		"Source Manual Medium",
		"Traffic medium from URL parameters. Extracted from 'utm_medium' in the page URL. Part of manual traffic source attribution.",
	),
)

var sourceManualTermColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualTerm.ID,
	ProtocolInterfaces.EventSourceManualTerm.Field,
	"utm_term",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualTerm.ID)),
	),
	columns.WithEventColumnDocs(
		"Source Manual Term",
		"Campaign term from URL parameters. Extracted from 'utm_term' in the page URL. Part of manual traffic source attribution.",
	),
)

var sourceManualContentColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualContent.ID,
	ProtocolInterfaces.EventSourceManualContent.Field,
	"utm_content",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualContent.ID)),
	),
	columns.WithEventColumnDocs(
		"Source Manual Content",
		"Campaign content from URL parameters. Extracted from 'utm_content' in the page URL. Part of manual traffic source attribution.", // nolint:lll // it's a description
	),
)

var sourceManualSourcePlatformColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualSourcePlatform.ID,
	ProtocolInterfaces.EventSourceManualSourcePlatform.Field,
	"utm_source_platform",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualSourcePlatform.ID)),
	),
	columns.WithEventColumnDocs(
		"Source Manual Source Platform",
		"Source platform from URL parameters. Extracted from 'utm_source_platform' in the page URL. Part of manual traffic source attribution.", // nolint:lll // it's a description
	),
)

var sourceManualCreativeFormatColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualCreativeFormat.ID,
	ProtocolInterfaces.EventSourceManualCreativeFormat.Field,
	"utm_creative_format",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualCreativeFormat.ID)),
	),
	columns.WithEventColumnDocs(
		"Source Manual Creative Format",
		"Creative format from URL parameters. Extracted from 'utm_creative_format' in the page URL. Part of manual traffic source attribution.", // nolint:lll // it's a description
	),
)

var sourceManualMarketingTacticColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceManualMarketingTactic.ID,
	ProtocolInterfaces.EventSourceManualMarketingTactic.Field,
	"utm_marketing_tactic",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceManualMarketingTactic.ID)),
	),
	columns.WithEventColumnDocs(
		"Source Manual Marketing Tactic",
		"Marketing tactic from URL parameters. Extracted from 'utm_marketing_tactic' in the page URL. Part of manual traffic source attribution.", // nolint:lll // it's a description
	),
)

var sourceGclidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceGclid.ID,
	ProtocolInterfaces.EventSourceGclid.Field,
	"gclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceGclid.ID)),
	),
	columns.WithEventColumnDocs(
		"Source GCLID",
		"Google Click ID from URL parameters. Extracted from 'gclid' in the page URL. Used for traffic source attribution from Google Ads.", // nolint:lll // it's a description
	),
)

var sourceDclidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceDclid.ID,
	ProtocolInterfaces.EventSourceDclid.Field,
	"dclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceDclid.ID)),
	),
	columns.WithEventColumnDocs(
		"Source DCLID",
		"Display Click ID from URL parameters. Extracted from 'dclid' in the page URL. Used for traffic source attribution from Google Display & Video 360.", // nolint:lll // it's a description
	),
)

var sourceSrsltidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventSourceSrsltid.ID,
	ProtocolInterfaces.EventSourceSrsltid.Field,
	"srsltid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventSourceSrsltid.ID)),
	),
	columns.WithEventColumnDocs(
		"Source SRSLTID",
		"Google Shopping Result ID from URL parameters. Extracted from 'srsltid' in the page URL. Used for traffic source attribution from Google Shopping.", // nolint:lll // it's a description
	),
)
