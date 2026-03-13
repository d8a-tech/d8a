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
func TestMatomoEventCoreColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildDeterministicTimeHit := func(t *testing.T) *hits.Hit {
		hit := hits.New()
		hit.EventName = pageViewEventType
		hit.PropertyID = "test_property_id"
		warsaw, err := time.LoadLocation("Europe/Warsaw")
		require.NoError(t, err)
		hit.MustParsedRequest().ServerReceivedTime = time.Date(2025, 1, 1, 1, 0, 0, 0, warsaw)
		return hit
	}

	buildPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := testHitOne()
		hit.EventName = pageViewEventType
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

	testCases := []testCase{
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
			name:        "EventPageTitle_Valid",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "action_name", "My Page")},
			fieldName:   "page_title",
			expected:    "My Page",
			description: "Valid page title via Matomo action_name parameter",
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			columntests.ColumnTestCase(
				t,
				tc.buildHits(t),
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
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
