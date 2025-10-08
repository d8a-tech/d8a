package ga4

import "github.com/d8a-tech/d8a/pkg/columns"

var clickIDsGclidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventClickIDGclid.ID,
	ProtocolInterfaces.EventClickIDGclid.Field,
	"gclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventClickIDGclid.ID)),
	),
)

var clickIDsDclidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventClickIDDclid.ID,
	ProtocolInterfaces.EventClickIDDclid.Field,
	"dclid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventClickIDDclid.ID)),
	),
)

var clickIDsSrsltidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventClickIDSrsltid.ID,
	ProtocolInterfaces.EventClickIDSrsltid.Field,
	"srsltid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventClickIDSrsltid.ID)),
	),
)

var clickIDsGbraidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventClickIDGbraid.ID,
	ProtocolInterfaces.EventClickIDGbraid.Field,
	"gbraid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventClickIDGbraid.ID)),
	),
)

var clickIDsWbraidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventClickIDWbraid.ID,
	ProtocolInterfaces.EventClickIDWbraid.Field,
	"wbraid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventClickIDWbraid.ID)),
	),
)

var clickIDsMsclkidColumn = columns.FromPageURLEventColumn(
	ProtocolInterfaces.EventClickIDMsclkid.ID,
	ProtocolInterfaces.EventClickIDMsclkid.Field,
	"msclkid",
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventClickIDMsclkid.ID)),
	),
)
