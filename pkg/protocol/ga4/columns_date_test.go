package ga4

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nolint:funlen,lll // test code
func TestEventDateColumns(t *testing.T) {
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
			name:        "Valid EventDateUTC",
			expected:    "2025-01-01",
			fieldName:   "date_utc",
			description: "Valid event date UTC",
		},
		{
			name:        "Valid EventTimestampUTC",
			expected:    "2025-01-01T00:00:00Z",
			fieldName:   "timestamp_utc",
			description: "Valid event timestamp UTC",
		},
		{
			name:        "Valid EventPageLoadHash",
			param:       "_p",
			value:       "1714732800000",
			expected:    "2024-05-03T10:40:00Z",
			fieldName:   "page_load_hash",
			description: "Valid event page load hash",
		},
		{
			name:        "Invalid EventPageLoadHash",
			param:       "_p",
			value:       "invalid",
			expected:    nil,
			fieldName:   "page_load_hash",
			description: "Invalid event page load hash",
		},
		{
			name:        "Empty EventPageLoadHash",
			param:       "_p",
			value:       "",
			expected:    nil,
			fieldName:   "page_load_hash",
			description: "Empty event page load hash",
		},
	}

	for _, tc := range eventColumnTestCases {
		t.Run(tc.name, func(t *testing.T) {
			hit := hits.New()
			hit.EventName = "foo_event"
			warsaw, err := time.LoadLocation("Europe/Warsaw")
			require.NoError(t, err)
			// 1 AM Warsaw time = midnight UTC (Warsaw is UTC+1 in January)
			hit.MustServerAttributes().ServerReceivedTime = time.Date(2025, 1, 1, 1, 0, 0, 0, warsaw)
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
