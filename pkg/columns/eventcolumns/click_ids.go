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
)

// ClickIDsDclidColumn is the column for the DCLID of an event
var ClickIDsDclidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventClickIDDclid.ID,
	columns.CoreInterfaces.EventClickIDDclid.Field,
	"dclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventClickIDDclid.ID)),
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
)

// ClickIDsGbraidColumn is the column for the GBRAID of an event
var ClickIDsGbraidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventClickIDGbraid.ID,
	columns.CoreInterfaces.EventClickIDGbraid.Field,
	"gbraid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventClickIDGbraid.ID)),
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
)

// ClickIDsMsclkidColumn is the column for the MSCLKID of an event
var ClickIDsMsclkidColumn = columns.FromPageURLEventColumn(
	columns.CoreInterfaces.EventClickIDMsclkid.ID,
	columns.CoreInterfaces.EventClickIDMsclkid.Field,
	"msclkid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.EventClickIDMsclkid.ID)),
	),
)
