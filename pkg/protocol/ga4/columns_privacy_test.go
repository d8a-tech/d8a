package ga4

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nolint:funlen,lll // test code
func TestEventPrivacyColumns(t *testing.T) {
	var eventColumnTestCases = []struct {
		name        string
		param       string
		value       string
		expected    any
		expectedErr bool
		fieldName   string
		description string
	}{
		{
			name:        "True EventPrivacyAnalyticsStorage",
			param:       "gcs",
			value:       "G001",
			expected:    true,
			fieldName:   "privacy_analytics_storage",
			description: "Valid event privacy analytics storage",
		},
		{
			name:        "False EventPrivacyAnalyticsStorage",
			param:       "gcs",
			value:       "G110",
			expected:    false,
			fieldName:   "privacy_analytics_storage",
			description: "Valid event privacy analytics storage",
		},
		{
			name:        "Missing EventPrivacyAnalyticsStorage",
			expected:    nil,
			fieldName:   "privacy_analytics_storage",
			description: "Empty event privacy analytics storage",
		},
		{
			name:        "Short EventPrivacyAnalyticsStorage",
			param:       "gcs",
			value:       "",
			expected:    nil,
			fieldName:   "privacy_analytics_storage",
			description: "Short event privacy analytics storage",
		},
		{
			name:        "Invalid EventPrivacyAnalyticsStorage",
			param:       "gcs",
			value:       "💩💩💩💩💩💩",
			expected:    nil,
			fieldName:   "privacy_analytics_storage",
			description: "Invalid event privacy analytics storage",
		},

		{
			name:        "True EventPrivacyAdsStorage",
			param:       "gcs",
			value:       "G010",
			expected:    true,
			fieldName:   "privacy_ads_storage",
			description: "Valid event privacy ads storage",
		},
		{
			name:        "False EventPrivacyAdsStorage",
			param:       "gcs",
			value:       "G101",
			expected:    false,
			fieldName:   "privacy_ads_storage",
			description: "Valid event privacy ads storage",
		},
		{
			name:        "Missing EventPrivacyAdsStorage",
			expected:    nil,
			fieldName:   "privacy_ads_storage",
			description: "Empty event privacy ads storage",
		},
		{
			name:        "Short EventPrivacyAdsStorage",
			param:       "gcs",
			value:       "",
			expected:    nil,
			fieldName:   "privacy_ads_storage",
			description: "Short event privacy ads storage",
		},
		{
			name:        "Invalid EventPrivacyAdsStorage",
			param:       "gcs",
			value:       "💩💩💩💩💩💩",
			expected:    nil,
			fieldName:   "privacy_ads_storage",
			description: "Invalid event privacy ads storage",
		},
	}

	for _, tc := range eventColumnTestCases {
		t.Run(tc.name, func(t *testing.T) {
			hit := hits.New()
			hit.EventName = "foo_event"
			EnsureValidTestHit(hit)
			// given
			columntests.ColumnTestCase(
				t,
				columntests.TestHits{hit},
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
				NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
				columntests.EnsureQueryParam(0, tc.param, tc.value))
		})
	}
}
