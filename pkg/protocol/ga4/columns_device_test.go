package ga4

import (
	"net/http"
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
func TestDeviceRelatedEventColumns(t *testing.T) {
	var eventColumnTestCases = []struct {
		name        string
		param       string
		value       string
		headers     http.Header
		expected    any
		expectedErr bool
		fieldName   string
		description string
	}{
		{
			name:        "Iphone_DeviceLanguageViaQueryParam",
			param:       "ul",
			value:       "en-us",
			expected:    "en-us",
			fieldName:   "device_language",
			description: "Valid device language via query param",
		},
		{
			name: "Iphone_DeviceLanguageViaHeader",
			headers: http.Header{
				"Accept-Language": []string{"en-gb"},
			},
			expected:    "en-gb",
			fieldName:   "device_language",
			description: "Valid device language via Accept-Language header fallback",
		},
	}

	for _, tc := range eventColumnTestCases {
		t.Run(tc.name, func(t *testing.T) {
			hit := hits.New()
			hit.EventName = PageViewEventType
			var cfg []columntests.CaseConfigFunc
			// Ensure query params are non-empty so param columns don't break the event (they cast nil as error).
			cfg = append(cfg, columntests.EnsureQueryParam(0, "v", "2"))
			for key, values := range tc.headers {
				for _, value := range values {
					cfg = append(cfg, columntests.EnsureHeader(0, key, value))
				}
			}
			if tc.param != "" {
				cfg = append(cfg, columntests.EnsureQueryParam(0, tc.param, tc.value))
			}
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
						require.NotEmpty(t, whd.WriteCalls, "expected at least one warehouse write call")
						require.NotEmpty(t, whd.WriteCalls[0].Records, "expected at least one record written")
						record := whd.WriteCalls[0].Records[0]
						assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
					}
				},
				NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
				cfg...)
		})
	}
}
