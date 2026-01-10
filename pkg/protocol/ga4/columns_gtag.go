package ga4

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
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
	func(_ *schema.Event, url *url.URL) (any, schema.D8AColumnWriteError) {
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
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		if len(event.BoundHit.MustParsedRequest().QueryParams) == 0 {
			return "", nil
		}
		originalURL := event.BoundHit.MustParsedRequest().QueryParams.Get("dl")
		if originalURL == "" {
			return "", nil
		}
		cleanedURL, _, err := columns.StripExcludedParams(originalURL)
		if err != nil {
			return nil, schema.NewBrokenEventError(fmt.Sprintf("failed to strip excluded params: %s", err))
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
	func(_ *schema.Event, url *url.URL) (any, schema.D8AColumnWriteError) {
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

var eventTrackingProtocolColumn = columns.ProtocolColumn(func(_ *schema.Event) (any, schema.D8AColumnWriteError) {
	return "ga4_gtag", nil
})

var eventPlatformColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventPlatform.ID,
	columns.CoreInterfaces.EventPlatform.Field,
	func(_ *schema.Event) (any, schema.D8AColumnWriteError) {
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

var sessionEngagementColumn = columns.NewSimpleSessionColumn(
	ProtocolInterfaces.SessionIsEngaged.ID,
	ProtocolInterfaces.SessionIsEngaged.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// Check if ANY event in the session is engaged
		for _, event := range session.Events {
			if event.BoundHit.MustParsedRequest().QueryParams == nil {
				continue
			}
			segValue := event.BoundHit.MustParsedRequest().QueryParams.Get("seg")
			if segValue == "" {
				continue
			}
			// Try to parse as int64
			casted, err := strconv.ParseInt(segValue, 10, 64)
			if err != nil {
				continue
			}
			// If any event has a non-zero engagement value, session is engaged
			if casted != 0 {
				return int64(1), nil
			}
		}
		return int64(0), nil
	},
	columns.WithSessionColumnRequired(false),
	columns.WithSessionColumnDocs(
		"Session Is Engaged",
		"Session engagement indicator. Set to 1 if ANY event in the session is engaged (sessions with meaningful user interaction).", // nolint:lll // it's a description
	),
)

var sessionReturningUserColumn = columns.NewSimpleSessionColumn(
	ProtocolInterfaces.SessionReturningUser.ID,
	ProtocolInterfaces.SessionReturningUser.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// Mark as returning if ANY event in the session has GA session number 2+.
		for _, event := range session.Events {
			raw, ok := event.Values[ProtocolInterfaces.GaSessionNumber.Field.Name]
			if !ok || raw == nil {
				continue
			}
			v, ok := raw.(int64)
			if !ok {
				continue
			}
			if v >= 2 {
				return int64(1), nil
			}
		}
		return int64(0), nil
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.GaSessionNumber.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnRequired(false),
	columns.WithSessionColumnDocs(
		"Session Returning User",
		"Returning user indicator. Set to 1 if any event in the session indicates that this is a subsequent session for the user. Derived from data extracted from the first-party cookie.", // nolint:lll // it's a description
	),
)

var sessionAbandonedCartColumn = columns.NewSimpleSessionColumn(
	ProtocolInterfaces.SessionAbandonedCart.ID,
	ProtocolInterfaces.SessionAbandonedCart.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		// Find the latest purchase event index
		latestPurchaseIndex := -1
		// Find all add_to_cart event indices
		addToCartIndices := []int{}

		for i, event := range session.Events {
			eventName, ok := event.Values[columns.CoreInterfaces.EventName.Field.Name]
			if !ok {
				continue
			}
			eventNameStr, ok := eventName.(string)
			if !ok {
				continue
			}

			switch eventNameStr {
			case PurchaseEventType:
				latestPurchaseIndex = i
			case AddToCartEventType:
				addToCartIndices = append(addToCartIndices, i)
			}
		}

		// If no add_to_cart events, cart is not abandoned
		if len(addToCartIndices) == 0 {
			return int64(0), nil
		}

		// If there's an add_to_cart but no purchase, cart is abandoned
		if latestPurchaseIndex == -1 {
			return int64(1), nil
		}

		// Find the latest add_to_cart event index
		latestAddToCartIndex := addToCartIndices[len(addToCartIndices)-1]

		// If the latest add_to_cart is after the latest purchase, cart is abandoned
		if latestAddToCartIndex > latestPurchaseIndex {
			return int64(1), nil
		}

		return int64(0), nil
	},
	columns.WithSessionColumnRequired(false),
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Abandoned Cart",
		"Session abandoned cart indicator. Set to 1 if there's an add_to_cart event but no purchase event, or if add_to_cart occurs after the latest purchase event.", // nolint:lll // it's a description
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
