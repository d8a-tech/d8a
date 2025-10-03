// package columntests contains tests for the columns package
package columntests

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventColumns(t *testing.T) {
	// Test cases for event columns with different values
	var eventColumnTestCases = []struct {
		name        string
		param       string
		value       string
		expected    any
		expectedErr bool
		fieldName   string
		description string
	}{
		// Required fields

		{
			name:        "EventGclid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&gclid=1337",
			expected:    "1337",
			fieldName:   "params_gclid",
			description: "Valid GCLID",
		},
		{
			name:        "EventGclid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "params_gclid",
			description: "Empty GCLID",
		},
		{
			name:        "EventGclid_Dl_Missing",
			param:       "blabla",
			value:       "1337",
			expected:    nil,
			fieldName:   "params_gclid",
			description: "dl is missing",
		},
		{
			name:        "EventAnid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&anid=1337",
			expected:    "1337",
			fieldName:   "params_anid",
			description: "Valid ANID",
		},
		{
			name:        "EventAnid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "params_anid",
			description: "Empty ANID",
		},
		{
			name:        "EventUtmMarketingTactic_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_marketing_tactic=1337",
			expected:    "1337",
			fieldName:   "utm_marketing_tactic",
			description: "Valid UTM marketing tactic",
		},
		{
			name:        "EventUtmMarketingTactic_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_marketing_tactic",
			description: "Empty UTM marketing tactic",
		},
		{
			name:        "EventUtmSourcePlatform_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_source_platform=1337",
			expected:    "1337",
			fieldName:   "utm_source_platform",
			description: "Valid UTM source platform",
		},
		{
			name:        "EventUtmSourcePlatform_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_source_platform",
			description: "Empty UTM source platform",
		},
		{
			name:        "EventUtmTerm_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_term=1337",
			expected:    "1337",
			fieldName:   "utm_term",
			description: "Valid UTM term",
		},
		{
			name:        "EventUtmTerm_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_term",
			description: "Empty UTM term",
		},
		{
			name:        "EventUtmContent_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_content=1337",
			expected:    "1337",
			fieldName:   "utm_content",
			description: "Valid UTM content",
		},
		{
			name:        "EventUtmContent_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_content",
			description: "Empty UTM content",
		},
		{
			name:        "EventUtmSource_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_source=1337",
			expected:    "1337",
			fieldName:   "utm_source",
			description: "Valid UTM source",
		},
		{
			name:        "EventUtmSource_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_source",
			description: "Empty UTM source",
		},
		{
			name:        "EventUtmMedium_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_medium=1337",
			expected:    "1337",
			fieldName:   "utm_medium",
			description: "Valid UTM medium",
		},
		{
			name:        "EventUtmMedium_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_medium",
			description: "Empty UTM medium",
		},
		{
			name:        "EventUtmCampaign_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_campaign=1337",
			expected:    "1337",
			fieldName:   "utm_campaign",
			description: "Valid UTM campaign",
		},
		{
			name:        "EventUtmCampaign_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_campaign",
			description: "Empty UTM campaign",
		},
		{
			name:        "EventUtmId_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_id=1337",
			expected:    "1337",
			fieldName:   "utm_id",
			description: "Valid UTM ID",
		},
		{
			name:        "EventUtmId_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_id",
			description: "Empty UTM ID",
		},
		{
			name:        "EventUtmCreativeFormat_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_creative_format=1337",
			expected:    "1337",
			fieldName:   "utm_creative_format",
			description: "Valid UTM creative format",
		},
		{
			name:        "EventUtmCreativeFormat_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_creative_format",
			description: "Empty UTM creative format",
		},
	}

	for _, tc := range eventColumnTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			ColumnTestCase(
				t,
				TestHits{TestHitOne()},
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					if tc.expectedErr {
						assert.Error(t, closeErr)
					} else {
						require.NoError(t, closeErr)
						record := whd.WriteCalls[0].Records[0]
						assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
					}
				},
				ga4.NewGA4Protocol(),
				EnsureQueryParam(0, tc.param, tc.value))
		})
	}
}

func TestSessionHitNumber(t *testing.T) {
	htThree := TestHitThree()
	htThree.QueryParams.Set("dl", "https%3A%2F%2Fd8a-tech.github.io%2Fanalytics-playground%2Ffoobar.html")
	ColumnTestCase(
		t,
		TestHits{TestHitOne(), TestHitTwo(), htThree},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			assert.Equal(t, int64(0), whd.WriteCalls[0].Records[0]["session_page_number"])
			assert.Equal(t, int64(0), whd.WriteCalls[0].Records[1]["session_page_number"])
			assert.Equal(t, int64(1), whd.WriteCalls[0].Records[2]["session_page_number"])

		},
		ga4.NewGA4Protocol(),
	)
}
