package matomo

import (
	"fmt"
	"net/url"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// eventIgnoreReferrerColumn reads the ignore_referrer (or its misspelled alias ignore_referer)
// query parameter and returns true when either is set to "1", false when present but not "1",
// and nil when neither is present.
var eventIgnoreReferrerColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventIgnoreReferrer.ID,
	columns.CoreInterfaces.EventIgnoreReferrer.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		params := event.BoundHit.MustParsedRequest().QueryParams
		if len(params) == 0 {
			return nil, nil //nolint:nilnil // no params present
		}
		_, hasReferrer := params["ignore_referrer"]
		_, hasReferer := params["ignore_referer"]
		if !hasReferrer && !hasReferer {
			return nil, nil //nolint:nilnil // param not present
		}
		return params.Get("ignore_referrer") == "1" || params.Get("ignore_referer") == "1", nil
	},
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Ignore Referrer",
		"Whether the referrer should be ignored for this hit. "+
			"True when ignore_referrer or ignore_referer query parameter is set to \"1\".",
	),
)

var eventDateUTCColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventDateUTC.ID,
	columns.CoreInterfaces.EventDateUTC.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		return event.BoundHit.MustParsedRequest().ServerReceivedTime.UTC().Format("2006-01-02"), nil
	},
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Event Date (UTC)",
		"The date when the event occurred in the UTC timezone, formatted as YYYY-MM-DD.",
	),
)

var eventTimestampUTCColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventTimestampUTC.ID,
	columns.CoreInterfaces.EventTimestampUTC.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		return event.BoundHit.MustParsedRequest().ServerReceivedTime.UTC().Format(time.RFC3339), nil
	},
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Event Timestamp (UTC)",
		"The precise UTC timestamp of when the event occurred, with second-level precision. This represents the time recorded when the hit is received by the server.", // nolint:lll // it's a description
	),
)

var eventPageReferrerColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageReferrer.ID,
	columns.CoreInterfaces.EventPageReferrer.Field,
	"urlref",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Page Referrer",
		"The URL of the page that referred the user to the current page, set to empty string for direct traffic or when referrer information is not available.", // nolint:lll // it's a description
	),
)

var eventPageTitleColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageTitle.ID,
	columns.CoreInterfaces.EventPageTitle.Field,
	"action_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Page Title",
		"The title of the page where the event occurred, as specified in the action_name parameter.",
	),
)

var eventPageLocationColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventPageLocation.ID,
	columns.CoreInterfaces.EventPageLocation.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		originalURL := event.BoundHit.MustParsedRequest().QueryParams.Get("url")
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
	func(_ *schema.Event, u *url.URL) (any, schema.D8AColumnWriteError) {
		return u.Hostname(), nil
	},
	columns.WithEventColumnDocs(
		"Page Hostname",
		"The hostname of the page where the event occurred, as specified in the URL (e.g., 'www.example.com', 'shop.example.com').", // nolint:lll // it's a description
	),
)

var eventPagePathColumn = columns.URLElementColumn(
	columns.CoreInterfaces.EventPagePath.ID,
	columns.CoreInterfaces.EventPagePath.Field,
	func(_ *schema.Event, u *url.URL) (any, schema.D8AColumnWriteError) {
		return u.Path, nil
	},
	columns.WithEventColumnDocs(
		"Page Path",
		"The path of the page where the event occurred, as specified in the URL (e.g., '/products/shoes', '/blog/article-name').", // nolint:lll // it's a description
	),
)

var eventTrackingProtocolColumn = columns.ProtocolColumn(func(_ *schema.Event) (any, schema.D8AColumnWriteError) {
	return "matomo", nil
})

var eventPlatformColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventPlatform.ID,
	columns.CoreInterfaces.EventPlatform.Field,
	func(_ *schema.Event) (any, schema.D8AColumnWriteError) {
		return columns.EventPlatformWeb, nil
	},
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Platform",
		"The platform from which the event was sent. Identifies whether the event originated from a website, mobile app, or another source (e.g., 'web', 'ios', or 'android').", // nolint:lll // it's a description
	),
)

var deviceLanguageColumn = columns.NewLanguageColumn(
	columns.CoreInterfaces.DeviceLanguage.ID,
	columns.CoreInterfaces.DeviceLanguage.Field,
	func(req *hits.ParsedRequest) (string, bool) {
		v := req.QueryParams.Get("lang")
		if v != "" {
			return v, true
		}
		return "", false
	},
	columns.WithEventColumnDocs(
		"Device Language",
		"The language setting of the user's device, extracted from the lang query parameter or Accept-Language header, based on ISO 639 standard for languages and ISO 3166 for country codes (e.g., 'en-us', 'en-gb', 'de-de').", // nolint:lll // it's a description
	),
)
