package ga4

import (
	"net/url"

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

var eventPageTitleColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageTitle.ID,
	columns.CoreInterfaces.EventPageTitle.Field,
	"dt",
	columns.WithEventColumnRequired(false),
)

var eventPageReferrerColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageReferrer.ID,
	columns.CoreInterfaces.EventPageReferrer.Field,
	"dr",
	columns.WithEventColumnRequired(false),
)

var eventPagePathColumn = columns.URLElementColumn(
	columns.CoreInterfaces.EventPagePath.ID,
	columns.CoreInterfaces.EventPagePath.Field,
	func(e *schema.Event, url *url.URL) (any, error) {
		return url.Path, nil
	},
)

var eventPageLocationColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageLocation.ID,
	columns.CoreInterfaces.EventPageLocation.Field,
	"dl",
	columns.WithEventColumnRequired(false),
)

var eventPageHostnameColumn = columns.URLElementColumn(
	columns.CoreInterfaces.EventPageHostname.ID,
	columns.CoreInterfaces.EventPageHostname.Field,
	func(e *schema.Event, url *url.URL) (any, error) {
		return url.Hostname(), nil
	},
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
