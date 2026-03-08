package matomo

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/columns"
)

var eventParamsContentInteractionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsContentInteraction.ID,
	ProtocolInterfaces.EventParamsContentInteraction.Field,
	"c_i",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsContentInteraction.ID)),
	),
	columns.WithEventColumnDocs(
		"Content Interaction",
		"The content interaction name, extracted from the c_i query parameter. "+
			"To track a content interaction, set c_i (typically together with c_n/c_p/c_t).",
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
		"Content Name",
		"The content name, extracted from the c_n query parameter. "+
			"To track a content impression, set c_n and optionally c_p and c_t.",
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
		"Content Piece",
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
		"Content Target",
		"The content target, extracted from the c_t query parameter.",
	),
)

// sessionTotalContentImpressionsColumn counts all content impression events in the session.
var sessionTotalContentImpressionsColumn = columns.TotalEventsOfGivenNameColumn(
	ProtocolInterfaces.SessionTotalContentImpressions.ID,
	ProtocolInterfaces.SessionTotalContentImpressions.Field,
	[]string{contentImpressionType},
	columns.WithSessionColumnDocs(
		"Total Content Impressions",
		fmt.Sprintf("The total number of content impressions (event name: %s) in the session. Uses Matomo content tracking semantics: set c_n and optionally c_p and c_t. See https://matomo.org/guide/reports/content-tracking/.", contentImpressionType), //nolint:lll // description
	),
)

// sessionTotalContentInteractionsColumn counts all content interaction events in the session.
var sessionTotalContentInteractionsColumn = columns.TotalEventsOfGivenNameColumn(
	ProtocolInterfaces.SessionTotalContentInteractions.ID,
	ProtocolInterfaces.SessionTotalContentInteractions.Field,
	[]string{contentInteractionType},
	columns.WithSessionColumnDocs(
		"Total Content Interactions",
		fmt.Sprintf("The total number of content interactions (event name: %s) in the session. Uses Matomo content tracking semantics: set c_i for interaction tracking. See https://matomo.org/guide/reports/content-tracking/.", contentInteractionType), //nolint:lll // description
	),
)
