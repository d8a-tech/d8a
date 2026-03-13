package matomo

import "github.com/d8a-tech/d8a/pkg/columns"

var eventParamsMediaAssetIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsMediaAssetID.ID,
	ProtocolInterfaces.EventParamsMediaAssetID.Field,
	"ma_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsMediaAssetID.ID)),
	),
	columns.WithEventColumnDocs(
		"Media Asset ID",
		"The raw media asset identifier, extracted from the ma_id query parameter.",
	),
)

var eventParamsMediaTypeColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsMediaType.ID,
	ProtocolInterfaces.EventParamsMediaType.Field,
	"ma_mt",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsMediaType.ID)),
	),
	columns.WithEventColumnDocs(
		"Media Type",
		"The raw media type or context, extracted from the ma_mt query parameter.",
	),
)
