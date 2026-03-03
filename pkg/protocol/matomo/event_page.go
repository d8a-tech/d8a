package matomo

import (
	"fmt"
	"net/url"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
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
