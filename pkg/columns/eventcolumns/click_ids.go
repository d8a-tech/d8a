package eventcolumns

import "github.com/d8a-tech/d8a/pkg/columns"

// ClickIDsGclidColumn is the column for the GCLID of an event
var ClickIDsGclidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventClickIDGclid.ID,
	columns.CoreInterfaces.EventClickIDGclid.Field,
	"gclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventClickIDGclid.ID)),
	),
	columns.WithEventColumnDocs(
		"Google gclid",
		"Google Click ID from Google Ads campaigns, extracted from the 'gclid' parameter in the page URL, used for tracking ad clicks and linking conversions to Google Ads.", // nolint:lll // it's a description
	),
)

// ClickIDsDclidColumn is the column for the DCLID of an event
var ClickIDsDclidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventClickIDDclid.ID,
	columns.CoreInterfaces.EventClickIDDclid.Field,
	"dclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventClickIDDclid.ID)),
	),
	columns.WithEventColumnDocs(
		"Google dclid",
		"Google Click ID from Google Display & Video 360 campaigns, extracted from the 'dclid' parameter in the page URL, used for tracking display ad clicks and conversions.", // nolint:lll // it's a description
	),
)

// ClickIDsSrsltidColumn is the column for the SRSLTID of an event
var ClickIDsSrsltidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventClickIDSrsltid.ID,
	columns.CoreInterfaces.EventClickIDSrsltid.Field,
	"srsltid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventClickIDSrsltid.ID)),
	),
	columns.WithEventColumnDocs(
		"Google srsltid",
		"Google Shopping Result Click ID, extracted from the 'srsltid' parameter in the page URL, used for tracking clicks from Google Shopping results.", // nolint:lll // it's a description
	),
)

// ClickIDsGbraidColumn is the column for the GBRAID of an event
var ClickIDsGbraidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventClickIDGbraid.ID,
	columns.CoreInterfaces.EventClickIDGbraid.Field,
	"gbraid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventClickIDGbraid.ID)),
	),
	columns.WithEventColumnDocs(
		"Google gbraid",
		"Google Click ID for iOS app-to-web conversions, extracted from the 'gbraid' parameter in the page URL, used for privacy-preserving attribution from iOS apps.", // nolint:lll // it's a description
	),
)

// ClickIDsWbraidColumn is the column for the WBRAID of an event
var ClickIDsWbraidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventClickIDWbraid.ID,
	columns.CoreInterfaces.EventClickIDWbraid.Field,
	"wbraid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventClickIDWbraid.ID)),
	),
	columns.WithEventColumnDocs(
		"Google wbraid",
		"Google Click ID for iOS web-to-app conversions, extracted from the 'wbraid' parameter in the page URL, used for privacy-preserving attribution to iOS apps.", // nolint:lll // it's a description
	),
)

// ClickIDsMsclkidColumn is the column for the MSCLKID of an event
var ClickIDsMsclkidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventClickIDMsclkid.ID,
	columns.CoreInterfaces.EventClickIDMsclkid.Field,
	"msclkid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventClickIDMsclkid.ID)),
	),
	columns.WithEventColumnDocs(
		"Microsoft msclkid",
		"Microsoft Click ID from Microsoft Advertising campaigns, extracted from the 'msclkid' parameter in the page URL, used for tracking Bing/Microsoft ad clicks and conversions.", // nolint:lll // it's a description
	),
)
