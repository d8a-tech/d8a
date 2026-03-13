package matomo

import "github.com/d8a-tech/d8a/pkg/columns"

var eventParamsPageViewIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsPageViewID.ID,
	ProtocolInterfaces.EventParamsPageViewID.Field,
	"pv_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsPageViewID.ID)),
	),
	columns.WithEventColumnDocs(
		"Page View ID",
		"The page view identifier, extracted from the pv_id query parameter.",
	),
)

var eventParamsGoalIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsGoalID.ID,
	ProtocolInterfaces.EventParamsGoalID.Field,
	"idgoal",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsGoalID.ID)),
	),
	columns.WithEventColumnDocs(
		"Goal ID",
		"The goal identifier, extracted from the idgoal query parameter.",
	),
)

var eventParamsCategoryColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsCategory.ID,
	ProtocolInterfaces.EventParamsCategory.Field,
	"e_c",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsCategory.ID)),
	),
	columns.WithEventColumnDocs(
		"Category",
		"The category of the event, extracted from the e_c query parameter.",
	),
)

var eventParamsActionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsAction.ID,
	ProtocolInterfaces.EventParamsAction.Field,
	"e_a",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsAction.ID)),
	),
	columns.WithEventColumnDocs(
		"Action",
		"The action of the event, extracted from the e_a query parameter.",
	),
)

var eventParamsValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsValue.ID,
	ProtocolInterfaces.EventParamsValue.Field,
	"e_v",
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamsValue.ID)),
	columns.WithEventColumnDocs(
		"Value",
		"The numeric value of the event, extracted from the e_v query parameter.",
	),
)

var eventMeasurementIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventMeasurementID.ID,
	ProtocolInterfaces.EventMeasurementID.Field,
	"idsite",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventMeasurementID.ID)),
	),
	columns.WithEventColumnDocs(
		"Measurement ID",
		"The Matomo measurement or tracking identifier, extracted from the idsite query parameter.",
	),
)
