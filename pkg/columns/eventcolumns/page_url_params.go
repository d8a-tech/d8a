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
