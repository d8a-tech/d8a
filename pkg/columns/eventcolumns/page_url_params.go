package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
)

// GclidColumn is the column for the GCLID tag of an event
var GclidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventGclid.ID,
	columns.CoreInterfaces.EventGclid.Field,
	"gclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventGclid.ID)),
	),
)

// DclidColumn is the column for the DCLID tag of an event
var DclidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventDclid.ID,
	columns.CoreInterfaces.EventDclid.Field,
	"dclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventDclid.ID)),
	),
)

// SrsltidColumn is the column for the SRSLTID tag of an event
var SrsltidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventSrsltid.ID,
	columns.CoreInterfaces.EventSrsltid.Field,
	"srsltid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventSrsltid.ID)),
	),
)

// AclidColumn is the column for the ACLID tag of an event
var AclidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventAclid.ID,
	columns.CoreInterfaces.EventAclid.Field,
	"aclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventAclid.ID)),
	),
)

// AnidColumn is the column for the ANID tag of an event
var AnidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventAnid.ID,
	columns.CoreInterfaces.EventAnid.Field,
	"anid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventAnid.ID)),
	),
)

// UtmMarketingTacticColumn is the column for the UTM marketing tactic of an event
var UtmMarketingTacticColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmMarketingTactic.ID,
	columns.CoreInterfaces.EventUtmMarketingTactic.Field,
	"utm_marketing_tactic",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmMarketingTactic.ID)),
	),
)

// UtmSourcePlatformColumn is the column for the UTM source platform of an event
var UtmSourcePlatformColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmSourcePlatform.ID,
	columns.CoreInterfaces.EventUtmSourcePlatform.Field,
	"utm_source_platform",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmSourcePlatform.ID)),
	),
)

// UtmTermColumn is the column for the UTM term of an event
var UtmTermColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmTerm.ID,
	columns.CoreInterfaces.EventUtmTerm.Field,
	"utm_term",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmTerm.ID)),
	),
)

// UtmContentColumn is the column for the UTM content of an event
var UtmContentColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmContent.ID,
	columns.CoreInterfaces.EventUtmContent.Field,
	"utm_content",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmContent.ID)),
	),
)

// UtmSourceColumn is the column for the UTM source of an event
var UtmSourceColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmSource.ID,
	columns.CoreInterfaces.EventUtmSource.Field,
	"utm_source",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmSource.ID)),
	),
)

// UtmMediumColumn is the column for the UTM medium of an event
var UtmMediumColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmMedium.ID,
	columns.CoreInterfaces.EventUtmMedium.Field,
	"utm_medium",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmMedium.ID)),
	),
)

// UtmCampaignColumn is the column for the UTM campaign of an event
var UtmCampaignColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmCampaign.ID,
	columns.CoreInterfaces.EventUtmCampaign.Field,
	"utm_campaign",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmCampaign.ID)),
	),
)

// UtmIDColumn is the column for the UTM ID of an event
var UtmIDColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmID.ID,
	columns.CoreInterfaces.EventUtmID.Field,
	"utm_id",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmID.ID)),
	),
)

// UtmCreativeFormatColumn is the column for the UTM creative format of an event
var UtmCreativeFormatColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventUtmCreativeFormat.ID,
	columns.CoreInterfaces.EventUtmCreativeFormat.Field,
	"utm_creative_format",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventUtmCreativeFormat.ID)),
	),
)
