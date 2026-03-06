package matomo

import "github.com/d8a-tech/d8a/pkg/columns"

var eventParamsContentInteractionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsContentInteraction.ID,
	ProtocolInterfaces.EventParamsContentInteraction.Field,
	"c_i",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsContentInteraction.ID)),
	),
	columns.WithEventColumnDocs(
		"Event Params Content Interaction",
		"The content interaction name, extracted from the c_i query parameter.",
	),
)

var eventParamsContentNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsContentName.ID,
	ProtocolInterfaces.EventParamsContentName.Field,
	"c_n",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsContentName.ID)),
	),
	columns.WithEventColumnDocs(
		"Event Params Content Name",
		"The content name, extracted from the c_n query parameter.",
	),
)

var eventParamsContentPieceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsContentPiece.ID,
	ProtocolInterfaces.EventParamsContentPiece.Field,
	"c_p",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsContentPiece.ID)),
	),
	columns.WithEventColumnDocs(
		"Event Params Content Piece",
		"The content piece, extracted from the c_p query parameter.",
	),
)

var eventParamsContentTargetColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsContentTarget.ID,
	ProtocolInterfaces.EventParamsContentTarget.Field,
	"c_t",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsContentTarget.ID)),
	),
	columns.WithEventColumnDocs(
		"Event Params Content Target",
		"The content target, extracted from the c_t query parameter.",
	),
)
