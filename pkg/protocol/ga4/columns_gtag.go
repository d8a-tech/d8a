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
	columns.WithEventColumnDocs(
		"Event Name",
		"The name of the event. This identifies the action the user performed (e.g., 'page_view', 'click', 'purchase', 'sign_up').", // nolint:lll // it's a description
	),
)

var eventPageTitleColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageTitle.ID,
	columns.CoreInterfaces.EventPageTitle.Field,
	"dt",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Page Title",
		"The title of the page where the event occurred, as specified in the HTML <title> tag.",
	),
)

var eventPageReferrerColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageReferrer.ID,
	columns.CoreInterfaces.EventPageReferrer.Field,
	"dr",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Page Referrer",
		"The URL of the page that referred the user to the current page, set to empty string for direct traffic or when referrer information is not available.", // nolint:lll // it's a description
	),
)

var eventPagePathColumn = columns.URLElementColumn(
	columns.CoreInterfaces.EventPagePath.ID,
	columns.CoreInterfaces.EventPagePath.Field,
	func(_ *schema.Event, url *url.URL) (any, error) {
		return url.Path, nil
	},
	columns.WithEventColumnDocs(
		"Page Path",
		"The path of the page where the event occurred, as specified in the URL (e.g., '/products/shoes', '/blog/article-name').", // nolint:lll // it's a description
	),
)

var eventPageLocationColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventPageLocation.ID,
	columns.CoreInterfaces.EventPageLocation.Field,
	func(event *schema.Event) (any, error) {
		if len(event.BoundHit.QueryParams) == 0 {
			return "", nil
		}
		originalURL := event.BoundHit.QueryParams.Get("dl")
		if originalURL == "" {
			return "", nil
		}
		cleanedURL, _, err := columns.StripExcludedParams(originalURL)
		if err != nil {
			return nil, err
		}
		columns.WriteOriginalPageLocation(event, originalURL)
		return cleanedURL, nil
	},
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Page Location",
		"The complete URL of the page where the event occurred, including protocol, domain, path, and query parameters (e.g., 'https://www.example.com/products/shoes?color=red&size=10'). Tracking parameters (UTM, click IDs) are excluded once extracted into dedicated columns.", // nolint:lll // it's a description
	),
)

var eventPageHostnameColumn = columns.URLElementColumn(
	columns.CoreInterfaces.EventPageHostname.ID,
	columns.CoreInterfaces.EventPageHostname.Field,
	func(_ *schema.Event, url *url.URL) (any, error) {
		return url.Hostname(), nil
	},
	columns.WithEventColumnDocs(
		"Page Hostname",
		"The hostname of the page where the event occurred, as specified in the URL (e.g., 'www.example.com', 'shop.example.com').", // nolint:lll // it's a description
	),
)

var eventIgnoreReferrerColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventIParamgnoreReferrer.ID,
	ProtocolInterfaces.EventIParamgnoreReferrer.Field,
	"ir",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.NilIfError(columns.CastToBool(ProtocolInterfaces.EventIParamgnoreReferrer.ID))),
	columns.WithEventColumnDocs(
		"Ignore Referrer",
		"Indicates whether to ignore the referrer information.", // nolint:lll // it's a description
	),
)

var eventTrackingProtocolColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventTrackingProtocol.ID,
	columns.CoreInterfaces.EventTrackingProtocol.Field,
	func(_ *schema.Event) (any, error) {
		return "ga4_gtag", nil
	},
	columns.WithEventColumnDocs(
		"Tracking Protocol",
		"The tracking protocol implementation used to send this event. Identifies which protocol parser processed the incoming hit (e.g., 'ga4_gtag', 'ga4_firebase').", // nolint:lll // it's a description
	),
)

var eventPlatformColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventPlatform.ID,
	columns.CoreInterfaces.EventPlatform.Field,
	func(_ *schema.Event) (any, error) {
		return columns.EventPlatformWeb, nil
	},
	columns.WithEventColumnDocs(
		"Platform",
		"The platform from which the event was sent. Identifies whether the event originated from a website, mobile app, or another source (e.g., 'web', 'ios', or 'android').", // nolint:lll // it's a description
	),
)

var eventEngagementTimeMsColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamEngagementTimeMs.ID,
	ProtocolInterfaces.EventParamEngagementTimeMs.Field,
	"_et",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamEngagementTimeMs.ID)),
	columns.WithEventColumnDocs(
		"Engagement Time (ms)",
		"The time in milliseconds a user was engaged with the app or page.", // nolint:lll // it's a description
	),
)

var eventGa4SessionIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.GaSessionID.ID,
	ProtocolInterfaces.GaSessionID.Field,
	"sid",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.GaSessionID.ID)),
	),
	columns.WithEventColumnDocs(
		"GA Session ID",
		"The Google Analytics 4 session identifier. A unique identifier for the current session, used by GA4 to group events into sessions. Extracted from the first-party cookie. Use only to compare numbers with GA4. For real session data calculated on the backend, use the session_id column. ", // nolint:lll // it's a description
	),
)

var eventGa4SessionNumberColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.GaSessionNumber.ID,
	ProtocolInterfaces.GaSessionNumber.Field,
	"sct",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.GaSessionNumber.ID)),
	columns.WithEventColumnDocs(
		"GA Session Number",
		"The Google Analytics 4 sequential count of sessions for this user. Increments with each new session (e.g., 1 for first session, 2 for second). Extracted from the first-party cookie. ", // nolint:lll // it's a description
	),
)

var sessionEngagementColumn = columns.FromQueryParamSessionColumn(
	ProtocolInterfaces.SessionIsEngaged.ID,
	ProtocolInterfaces.SessionIsEngaged.Field,
	"seg",
	columns.WithSessionColumnRequired(false),
	columns.WithSessionColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.SessionIsEngaged.ID)),
	columns.WithSessionColumnDocs(
		"Session Is Engaged",
		"Session engagement indicator. Typically set to 1 for engaged sessions (sessions with meaningful user interaction).", // nolint:lll // it's a description
	),
)

var gtmDebugColumn = columns.FromPageURLParamEventColumn(
	ProtocolInterfaces.EventGtmDebug.ID,
	ProtocolInterfaces.EventGtmDebug.Field,
	"gtm_debug",
	false,
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventGtmDebug.ID)),
	),
	columns.WithEventColumnDocs(
		"GTM Debug",
		"The Tag Manager debug mode identifier, present when the Tag Manager is running in debug/preview mode for testing.",
	),
)

var eventMeasurementIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventMeasurementID.ID,
	ProtocolInterfaces.EventMeasurementID.Field,
	"tid",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventMeasurementID.ID)),
	),
	columns.WithEventColumnDocs(
		"Measurement ID",
		"The Google Analytics 4 measurement / tracking identifier. A unique identifier for the property that sent this event. Extracted from the first-party cookie. Use only to compare numbers with GA4. For real property data calculated on the backend, use the property_id column. ", // nolint:lll // it's a description
	),
)
