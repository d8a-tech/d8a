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
	const iphoneUA = `Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1`

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
			name: "Desktop_DeviceCategory",
			headers: http.Header{
				"User-Agent": []string{
					"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"},
			},
			expected:    "desktop",
			fieldName:   "device_category",
			description: "Valid device category",
		},
		{
			name: "Iphone_DeviceCategory",
			headers: http.Header{
				"User-Agent": []string{iphoneUA},
			},
			expected:    "smartphone", // dd2 v1.0.2 returns "smartphone" for iPhone UA
			fieldName:   "device_category",
			description: "Valid device category",
		},
		{
			name: "Iphone_DeviceMobileBrandName",
			headers: http.Header{
				"User-Agent": []string{iphoneUA},
			},
			expected:    "Apple",
			fieldName:   "device_mobile_brand_name",
			description: "Valid device mobile brand name",
		},
		{
			name: "Iphone_DeviceMobileModelNameViaHeader",
			headers: http.Header{
				"User-Agent": []string{iphoneUA},
			},
			expected:    "iPhone",
			fieldName:   "device_mobile_model_name",
			description: "Valid device mobile model name",
		},
		{
			name: "Iphone_DeviceOperatingSystemViaHeader",
			headers: http.Header{
				"User-Agent": []string{iphoneUA},
			},
			expected:    "iOS",
			fieldName:   "device_operating_system",
			description: "Valid device operating system via header",
		},
		{
			name: "Iphone_DeviceOperatingSystemVersionViaHeader",
			headers: http.Header{
				"User-Agent": []string{iphoneUA},
			},
			expected:    "11.0",
			fieldName:   "device_operating_system_version",
			description: "Valid device operating system version via header",
		},
		{
			name: "Iphone_DeviceLanguageViaHeader",
			headers: http.Header{
				"Accept-Language": []string{"en-us"},
			},
			expected:    "en-us",
			fieldName:   "device_language",
			description: "Valid device language via header",
		},
		{
			name: "Iphone_DeviceWebBrowserViaHeader",
			headers: http.Header{
				"User-Agent": []string{iphoneUA},
			},
			expected:    "Mobile Safari",
			fieldName:   "device_web_browser",
			description: "Valid device web browser via header",
		},
		{
			name: "Iphone_DeviceWebBrowserVersionViaHeader",
			headers: http.Header{
				"User-Agent": []string{iphoneUA},
			},
			expected:    "11.0",
			fieldName:   "device_web_browser_version",
			description: "Valid device web browser version via header",
		},
		// No info
		{
			name:        "Nil_DeviceCategory",
			headers:     http.Header{},
			expected:    nil,
			fieldName:   "device_category",
			description: "Valid device category",
		},
		{
			name:        "Nil_DeviceMobileBrandName",
			headers:     http.Header{},
			expected:    nil,
			fieldName:   "device_mobile_brand_name",
			description: "Valid device mobile brand name",
		},
		{
			name:        "Nil_DeviceMobileModelName",
			headers:     http.Header{},
			expected:    nil,
			fieldName:   "device_mobile_model_name",
			description: "Valid device mobile model name",
		},
		{
			name:        "Nil_DeviceOperatingSystem",
			headers:     http.Header{},
			expected:    nil,
			fieldName:   "device_operating_system",
			description: "Valid device operating system",
		},
		{
			name:        "Nil_DeviceOperatingSystemVersion",
			headers:     http.Header{},
			expected:    nil,
			fieldName:   "device_operating_system_version",
			description: "Valid device operating system version",
		},
		{
			name:        "Nil_DeviceLanguage",
			headers:     http.Header{},
			expected:    nil,
			fieldName:   "device_language",
			description: "Valid device language",
		},
		{
			name:        "Nil_DeviceWebBrowser",
			headers:     http.Header{},
			expected:    nil,
			fieldName:   "device_web_browser",
			description: "Valid device web browser",
		},
		{
			name:        "Nil_DeviceWebBrowserVersion",
			headers:     http.Header{},
			expected:    nil,
			fieldName:   "device_web_browser_version",
			description: "Valid device web browser version",
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
