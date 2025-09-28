package ga4

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var eventNameColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventName.ID,
	columns.CoreInterfaces.EventName.Field,
	"en",
	columns.WithEventColumnRequired(true),
	columns.WithEventColumnCast(columns.StrErrIfEmpty(columns.CoreInterfaces.EventName.ID)),
)

var eventDocumentTitleColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventDocumentTitle.ID,
	columns.CoreInterfaces.EventDocumentTitle.Field,
	"dt",
	columns.WithEventColumnRequired(false),
)

var eventDocumentLocationColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventDocumentLocation.ID,
	columns.CoreInterfaces.EventDocumentLocation.Field,
	"dl",
	columns.WithEventColumnRequired(false),
)

var eventDocumentReferrerColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventDocumentReferrer.ID,
	columns.CoreInterfaces.EventDocumentReferrer.Field,
	"dr",
	columns.WithEventColumnRequired(false),
)

var eventIgnoreReferrerColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventIgnoreReferrer.ID,
	ProtocolInterfaces.EventIgnoreReferrer.Field,
	"ir",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventIgnoreReferrer.ID))),
)

var eventTrackingProtocolColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventTrackingProtocol.ID,
	columns.CoreInterfaces.EventTrackingProtocol.Field,
	func(_ *schema.Event) (any, error) {
		return "ga4_gtag", nil
	},
)

var eventPlatformColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventPlatform.ID,
	columns.CoreInterfaces.EventPlatform.Field,
	func(_ *schema.Event) (any, error) {
		return columns.EventPlatformWeb, nil
	},
)

var eventEngagementTimeMsColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventEngagementTimeMs.ID,
	ProtocolInterfaces.EventEngagementTimeMs.Field,
	"_et",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventEngagementTimeMs.ID)),
)

var eventPageLocationColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageLocation.ID,
	columns.CoreInterfaces.EventPageLocation.Field,
	"dl",
	columns.WithEventColumnRequired(false),
)

var sessionGa4SessionIDColumn = columns.FromQueryParamSessionColumn(
	ProtocolInterfaces.SessionParamsGaSessionID.ID,
	ProtocolInterfaces.SessionParamsGaSessionID.Field,
	"sid",
	columns.WithSessionColumnRequired(false),
)

var sessionNumberColumn = columns.FromQueryParamSessionColumn(
	ProtocolInterfaces.SessionNumber.ID,
	ProtocolInterfaces.SessionNumber.Field,
	"sct",
	columns.WithSessionColumnRequired(false),
	columns.WithSessionColumnCast(columns.CastToInt64OrZero(ProtocolInterfaces.SessionNumber.ID)),
)

var sessionEngagementColumn = columns.FromQueryParamSessionColumn(
	ProtocolInterfaces.SessionEngagement.ID,
	ProtocolInterfaces.SessionEngagement.Field,
	"seg",
	columns.WithSessionColumnRequired(false),
	columns.WithSessionColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.SessionEngagement.ID)),
)
