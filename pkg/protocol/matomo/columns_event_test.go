package matomo

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nolint:funlen,lll // test code
func TestMatomoEventColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildDeterministicTimeHit := func(t *testing.T) *hits.Hit {
		hit := hits.New()
		hit.EventName = "page_view"
		hit.PropertyID = "test_property_id"
		warsaw, err := time.LoadLocation("Europe/Warsaw")
		require.NoError(t, err)
		// 1 AM Warsaw time = midnight UTC (Warsaw is UTC+1 in January)
		hit.MustParsedRequest().ServerReceivedTime = time.Date(2025, 1, 1, 1, 0, 0, 0, warsaw)
		return hit
	}

	buildPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := columntests.TestHitOne()
		hit.EventName = "page_view"
		return hit
	}

	buildNonPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := columntests.TestHitOne()
		hit.EventName = "scroll"
		return hit
	}

	single := func(build func(*testing.T) *hits.Hit) func(*testing.T) columntests.TestHits {
		return func(t *testing.T) columntests.TestHits {
			return columntests.TestHits{build(t)}
		}
	}

	type testCase struct {
		name        string
		buildHits   func(t *testing.T) columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		expectNoIO  bool
		description string
	}

	mergeCases := func(groups ...[]testCase) []testCase {
		total := 0
		for _, g := range groups {
			total += len(g)
		}
		out := make([]testCase, 0, total)
		for _, g := range groups {
			out = append(out, g...)
		}
		return out
	}

	clickIDCases := func(fieldName, urlParam, clickIDValue string) []testCase {
		return []testCase{
			{
				name: fieldName + "_PageViewWithValue",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildPageViewHit(t)}
				},
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/?"+urlParam+"="+clickIDValue)},
				fieldName:   fieldName,
				expected:    clickIDValue,
				description: "Returns click ID value from the first page view event",
			},
			{
				name: fieldName + "_SecondHitIsPageViewWithValue",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildNonPageViewHit(t), buildPageViewHit(t)}
				},
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(1, "url", "https://example.com/?"+urlParam+"="+clickIDValue),
					columntests.EnsureQueryParam(1, "v", "2"),
				},
				fieldName:   fieldName,
				expected:    clickIDValue,
				description: "Returns click ID from the first page view even when preceded by a non-page-view event",
			},
			{
				name: fieldName + "_NoPageViewEvents",
				buildHits: func(t *testing.T) columntests.TestHits {
					return columntests.TestHits{buildNonPageViewHit(t)}
				},
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/?"+urlParam+"="+clickIDValue)},
				fieldName:   fieldName,
				expected:    nil,
				description: "Returns nil when there are no page view events in the session",
			},
		}
	}

	testCases := mergeCases(
		[]testCase{
			{
				name:        "EventIgnoreReferrer_TrueViaReferrer",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ignore_referrer", "1")},
				fieldName:   "ignore_referrer",
				expected:    true,
				description: "ignore_referrer=1 returns true",
			},
			{
				name:        "EventIgnoreReferrer_TrueViaReferer",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ignore_referer", "1")},
				fieldName:   "ignore_referrer",
				expected:    true,
				description: "ignore_referer=1 (misspelled alias) returns true",
			},
			{
				name:        "EventIgnoreReferrer_FalseWhenZero",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "ignore_referrer", "0")},
				fieldName:   "ignore_referrer",
				expected:    false,
				description: "ignore_referrer=0 returns false",
			},
			{
				name:        "EventIgnoreReferrer_NilWhenAbsent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "ignore_referrer",
				expected:    nil,
				description: "Returns nil when neither ignore_referrer nor ignore_referer is present",
			},
			{
				name:        "EventDateUTC_Valid",
				buildHits:   single(buildDeterministicTimeHit),
				fieldName:   "date_utc",
				expected:    "2025-01-01",
				description: "Valid event date UTC",
			},
			{
				name:        "EventTimestampUTC_Valid",
				buildHits:   single(buildDeterministicTimeHit),
				fieldName:   "timestamp_utc",
				expected:    "2025-01-01T00:00:00Z",
				description: "Valid event timestamp UTC",
			},
			{
				name:        "EventPageReferrer_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "urlref", "https://example.com")},
				fieldName:   "page_referrer",
				expected:    "https://example.com",
				description: "Valid page referrer via Matomo urlref parameter",
			},
			{
				name:        "EventPageReferrer_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "urlref", "")},
				fieldName:   "page_referrer",
				expected:    "",
				description: "Empty page referrer via Matomo urlref parameter",
			},
			{
				name:        "EventPageTitle_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "action_name", "My Page")},
				fieldName:   "page_title",
				expected:    "My Page",
				description: "Valid page title via Matomo action_name parameter",
			},
			{
				name:        "EventPageTitle_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "action_name", "")},
				fieldName:   "page_title",
				expected:    "",
				description: "Empty page title via Matomo action_name parameter",
			},
			{
				name:        "EventPageLocation_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/path?foo=bar")},
				fieldName:   "page_location",
				expected:    "https://example.com/path?foo=bar",
				description: "Valid page location via Matomo url parameter",
			},
			{
				name:        "EventPageLocation_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "")},
				fieldName:   "page_location",
				expected:    "",
				description: "Empty page location via Matomo url parameter",
			},
			{
				name:        "EventPageLocation_BrokenURL",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "://bad")},
				fieldName:   "page_location",
				expectNoIO:  true,
				description: "Broken page location results in filtered-out session/event (no warehouse writes)",
			},
			{
				name:        "EventPageHostname_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/path")},
				fieldName:   "page_hostname",
				expected:    "example.com",
				description: "Valid page hostname derived from page_location",
			},
			{
				name:        "EventPagePath_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/my/path")},
				fieldName:   "page_path",
				expected:    "/my/path",
				description: "Valid page path derived from page_location",
			},
			{
				name:        "EventTrackingProtocol",
				buildHits:   single(buildPageViewHit),
				fieldName:   "tracking_protocol",
				expected:    "matomo",
				description: "Tracking protocol is constant matomo",
			},
			{
				name:        "EventPlatform",
				buildHits:   single(buildPageViewHit),
				fieldName:   "platform",
				expected:    "web",
				description: "Platform is constant web",
			},
			{
				name:        "DeviceLanguage_ViaParam",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "lang", "fr-fr")},
				fieldName:   "device_language",
				expected:    "fr-fr",
				description: "Device language via lang query parameter",
			},
			{
				name:      "DeviceLanguage_ViaHeader",
				buildHits: single(buildPageViewHit),
				cfg: []columntests.CaseConfigFunc{
					columntests.EnsureQueryParam(0, "lang", ""),
					columntests.EnsureHeader(0, "Accept-Language", "de-de"),
				},
				fieldName:   "device_language",
				expected:    "de-de",
				description: "Device language via Accept-Language header fallback",
			},
		},
		[]testCase{
			{
				name:        "EventParamsCategory_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_c", "checkout")},
				fieldName:   "params_category",
				expected:    "checkout",
				description: "Valid params category via e_c query parameter",
			},
			{
				name:        "EventParamsCategory_Empty",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_category",
				expected:    nil,
				description: "Returns nil when e_c parameter is absent",
			},
			{
				name:        "EventParamsAction_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_a", "add_to_cart")},
				fieldName:   "params_action",
				expected:    "add_to_cart",
				description: "Valid params action via e_a query parameter",
			},
			{
				name:        "EventParamsAction_Empty",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_action",
				expected:    nil,
				description: "Returns nil when e_a parameter is absent",
			},
			{
				name:        "EventParamsValue_ValidNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "99.95")},
				fieldName:   "params_value",
				expected:    99.95,
				description: "Valid numeric params value via e_v query parameter",
			},
			{
				name:        "EventParamsValue_ValidInteger",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "42")},
				fieldName:   "params_value",
				expected:    42.0,
				description: "Valid integer params value converted to float64",
			},
			{
				name:        "EventParamsValue_NonNumeric",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "not_a_number")},
				fieldName:   "params_value",
				expected:    nil,
				description: "Returns nil when e_v is not parseable as float64",
			},
			{
				name:        "EventParamsValue_Empty",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "e_v", "")},
				fieldName:   "params_value",
				expected:    nil,
				description: "Returns nil when e_v parameter is empty",
			},
			{
				name:        "EventParamsValue_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_value",
				expected:    nil,
				description: "Returns nil when e_v parameter is absent",
			},
		},
		clickIDCases("session_click_id_gclid", "gclid", "gclid_test_123"),
		clickIDCases("session_click_id_dclid", "dclid", "dclid_test_123"),
		clickIDCases("session_click_id_gbraid", "gbraid", "gbraid_test_123"),
		clickIDCases("session_click_id_srsltid", "srsltid", "srsltid_test_123"),
		clickIDCases("session_click_id_wbraid", "wbraid", "wbraid_test_123"),
		clickIDCases("session_click_id_fbclid", "fbclid", "fbclid_test_123"),
		clickIDCases("session_click_id_msclkid", "msclkid", "msclkid_test_123"),
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				tc.buildHits(t),
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					if tc.expectNoIO {
						require.Empty(t, whd.WriteCalls, "expected no warehouse write calls")
						return
					}
					require.NotEmpty(t, whd.WriteCalls, "expected at least one warehouse write call")
					require.NotEmpty(t, whd.WriteCalls[0].Records, "expected at least one record written")
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}
