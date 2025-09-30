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
