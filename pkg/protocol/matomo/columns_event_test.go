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

	buildDefaultHit := func(_ *testing.T) *hits.Hit {
		hit := columntests.TestHitOne()
		hit.EventName = "page_view"
		return hit
	}

	var testCases = []struct {
		name        string
		buildHit    func(t *testing.T) *hits.Hit
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		expectNoIO  bool
		description string
	}{
		{
			name:        "EventIgnoreReferrer_AlwaysNil",
			buildHit:    buildDefaultHit,
			fieldName:   "ignore_referrer",
			expected:    nil,
			description: "Ignore referrer is always nil for Matomo",
		},
		{
			name:        "EventDateUTC_Valid",
			buildHit:    buildDeterministicTimeHit,
			fieldName:   "date_utc",
			expected:    "2025-01-01",
			description: "Valid event date UTC",
		},
		{
			name:        "EventTimestampUTC_Valid",
			buildHit:    buildDeterministicTimeHit,
			fieldName:   "timestamp_utc",
			expected:    "2025-01-01T00:00:00Z",
			description: "Valid event timestamp UTC",
		},
		{
			name:        "EventPageReferrer_Valid",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "urlref", "https://example.com")},
			fieldName:   "page_referrer",
			expected:    "https://example.com",
			description: "Valid page referrer via Matomo urlref parameter",
		},
		{
			name:        "EventPageReferrer_Empty",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "urlref", "")},
			fieldName:   "page_referrer",
			expected:    "",
			description: "Empty page referrer via Matomo urlref parameter",
		},
		{
			name:        "EventPageTitle_Valid",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "action_name", "My Page")},
			fieldName:   "page_title",
			expected:    "My Page",
			description: "Valid page title via Matomo action_name parameter",
		},
		{
			name:        "EventPageTitle_Empty",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "action_name", "")},
			fieldName:   "page_title",
			expected:    "",
			description: "Empty page title via Matomo action_name parameter",
		},
		{
			name:        "EventPageLocation_Valid",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/path?foo=bar")},
			fieldName:   "page_location",
			expected:    "https://example.com/path?foo=bar",
			description: "Valid page location via Matomo url parameter",
		},
		{
			name:        "EventPageLocation_Empty",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "")},
			fieldName:   "page_location",
			expected:    "",
			description: "Empty page location via Matomo url parameter",
		},
		{
			name:        "EventPageLocation_BrokenURL",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "://bad")},
			fieldName:   "page_location",
			expectNoIO:  true,
			description: "Broken page location results in filtered-out session/event (no warehouse writes)",
		},
		{
			name:        "EventPageHostname_Valid",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/path")},
			fieldName:   "page_hostname",
			expected:    "example.com",
			description: "Valid page hostname derived from page_location",
		},
		{
			name:        "EventPagePath_Valid",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "url", "https://example.com/my/path")},
			fieldName:   "page_path",
			expected:    "/my/path",
			description: "Valid page path derived from page_location",
		},
		{
			name:        "EventTrackingProtocol",
			buildHit:    buildDefaultHit,
			fieldName:   "tracking_protocol",
			expected:    "matomo",
			description: "Tracking protocol is constant matomo",
		},
		{
			name:        "EventPlatform",
			buildHit:    buildDefaultHit,
			fieldName:   "platform",
			expected:    "web",
			description: "Platform is constant web",
		},
		{
			name:        "DeviceLanguage_ViaParam",
			buildHit:    buildDefaultHit,
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "lang", "fr-fr")},
			fieldName:   "device_language",
			expected:    "fr-fr",
			description: "Device language via lang query parameter",
		},
		{
			name:     "DeviceLanguage_ViaHeader",
			buildHit: buildDefaultHit,
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
			hit := tc.buildHit(t)
			// given
			columntests.ColumnTestCase(
				t,
				columntests.TestHits{hit},
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					if tc.expectNoIO {
						for _, call := range whd.WriteCalls {
							assert.Empty(t, call.Records, "expected no records written")
						}
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
