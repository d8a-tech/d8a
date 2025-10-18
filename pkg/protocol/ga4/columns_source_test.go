package ga4

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceColumns(t *testing.T) {
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
		// App params
		{
			name:        "EventSourceManualCampaignID_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_id=1337",
			expected:    "1337",
			fieldName:   "utm_id",
			description: "Valid UTM ID",
		},
		{
			name:        "EventSourceManualCampaignID_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_id",
			description: "Empty UTM ID",
		},
		{
			name:        "EventSourceManualCampaignName_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_campaign=1337",
			expected:    "1337",
			fieldName:   "utm_campaign",
			description: "Valid UTM campaign",
		},
		{
			name:        "EventSourceManualCampaignName_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_campaign",
			description: "Empty UTM campaign",
		},
		{
			name:        "EventSourceManualSource_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_source=1337",
			expected:    "1337",
			fieldName:   "utm_source",
			description: "Valid UTM source",
		},
		{
			name:        "EventSourceManualSource_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_source",
			description: "Empty UTM source",
		},
		{
			name:        "EventSourceManualMedium_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_medium=1337",
			expected:    "1337",
			fieldName:   "utm_medium",
			description: "Valid UTM medium",
		},
		{
			name:        "EventSourceManualMedium_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_medium",
			description: "Empty UTM medium",
		},
		{
			name:        "EventSourceManualTerm_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_term=1337",
			expected:    "1337",
			fieldName:   "utm_term",
			description: "Valid UTM term",
		},
		{
			name:        "EventSourceManualTerm_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_term",
			description: "Empty UTM term",
		},
		{
			name:        "EventSourceManualContent_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_content=1337",
			expected:    "1337",
			fieldName:   "utm_content",
			description: "Valid UTM content",
		},
		{
			name:        "EventSourceManualContent_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_content",
			description: "Empty UTM content",
		},
		{
			name:        "EventSourceManualSourcePlatform_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_source_platform=1337",
			expected:    "1337",
			fieldName:   "utm_source_platform",
			description: "Valid UTM source platform",
		},
		{
			name:        "EventSourceManualSourcePlatform_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_source_platform",
			description: "Empty UTM source platform",
		},
		{
			name:        "EventSourceManualCreativeFormat_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_creative_format=1337",
			expected:    "1337",
			fieldName:   "utm_creative_format",
			description: "Valid UTM creative format",
		},
		{
			name:        "EventSourceManualCreativeFormat_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_creative_format",
			description: "Empty UTM creative format",
		},
		{
			name:        "EventSourceManualMarketingTactic_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&utm_marketing_tactic=1337",
			expected:    "1337",
			fieldName:   "utm_marketing_tactic",
			description: "Valid UTM marketing tactic",
		},
		{
			name:        "EventSourceManualMarketingTactic_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "utm_marketing_tactic",
			description: "Empty UTM marketing tactic",
		},
		{
			name:        "EventSourceGclid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&gclid=1337",
			expected:    "1337",
			fieldName:   "source_gclid",
			description: "Valid GCLID",
		},
		{
			name:        "EventSourceGclid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "source_gclid",
			description: "Empty GCLID",
		},
		{
			name:        "EventSourceDclid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&dclid=1337",
			expected:    "1337",
			fieldName:   "source_dclid",
			description: "Valid DCLID",
		},
		{
			name:        "EventSourceDclid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "source_dclid",
			description: "Empty DCLID",
		},
		{
			name:        "EventSourceSrsltid_Valid",
			param:       "dl",
			value:       "https://example.com/page?foo=bar&srsltid=1337",
			expected:    "1337",
			fieldName:   "source_srsltid",
			description: "Valid SRSLTID",
		},
		{
			name:        "EventSourceSrsltid_Empty",
			param:       "dl",
			value:       "https://example.com/page?foo=bar",
			expected:    nil,
			fieldName:   "source_srsltid",
			description: "Empty SRSLTID",
		},
	}

	for _, tc := range eventColumnTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				columntests.TestHits{columntests.TestHitOne()},
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
				NewGA4Protocol(currency.NewDummyConverter(1)),
				columntests.EnsureQueryParam(0, tc.param, tc.value))
		})
	}
}
