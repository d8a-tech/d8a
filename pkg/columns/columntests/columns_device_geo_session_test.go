package columntests

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	deviceGeoSessionIPhoneUA = "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) " +
		"AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1"
	deviceGeoSessionDesktopUA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
	deviceGeoSessionDesktopCH = `"Linux"`
)

func TestDeviceGeoSessionColumns(t *testing.T) {
	proto := ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry())
	registry := columnset.DefaultColumnRegistry(
		proto,
		properties.NewTestSettingRegistry(),
		columnset.WithGeoProvider(dbip.NewStaticLookupProvider(&dbip.LookupResult{
			City:      "Paris",
			Region:    "Ile-de-France",
			Country:   "France",
			Continent: "Europe",
		}, nil)),
		columnset.WithDeviceDetectionColumns(eventcolumns.DD2Columns()),
	)

	testCases := []struct {
		name         string
		hits         TestHits
		config       []CaseConfigFunc
		expectations map[string]any
	}{
		{
			name: "single client session mirrors selected event values",
			hits: TestHits{TestHitOne()},
			config: []CaseConfigFunc{
				SetColumnsRegistry(registry),
				setClientID(0, "client-a"),
				setDeviceProfile(0, deviceProfile{
					userAgent:          deviceGeoSessionIPhoneUA,
					queryParamPlatform: "iOS",
				}),
				EnsureQueryParam(0, "ul", "fr-fr"),
				EnsureQueryParam(0, "sr", "390x844"),
			},
			expectations: map[string]any{
				"session_geo_city":                        "Paris",
				"session_geo_region":                      "Ile-de-France",
				"session_geo_metro":                       nil,
				"session_geo_country":                     "France",
				"session_geo_continent":                   "Europe",
				"session_geo_sub_continent":               nil,
				"session_device_category":                 "smartphone",
				"session_device_language":                 "fr-fr",
				"session_device_mobile_brand_name":        "Apple",
				"session_device_mobile_model_name":        "iPhone",
				"session_device_operating_system":         "iOS",
				"session_device_operating_system_version": "11.0",
				"session_device_web_browser":              "Mobile Safari",
				"session_device_web_browser_version":      "11.0",
				"session_device_screen_resolution":        "390x844",
			},
		},
		{
			name: "dominant client session uses dominant client first event values",
			hits: TestHits{TestHitOne(), TestHitTwo(), TestHitThree()},
			config: []CaseConfigFunc{
				SetColumnsRegistry(registry),
				setClientID(0, "client-a"),
				setDeviceProfile(0, deviceProfile{
					userAgent:          deviceGeoSessionDesktopUA,
					clientHintPlatform: deviceGeoSessionDesktopCH,
					queryParamPlatform: "Linux",
				}),
				EnsureQueryParam(0, "ul", "en-us"),
				EnsureQueryParam(0, "sr", "1024x768"),
				setClientID(1, "client-b"),
				setDeviceProfile(1, deviceProfile{
					userAgent:          deviceGeoSessionIPhoneUA,
					queryParamPlatform: "iOS",
				}),
				EnsureQueryParam(1, "ul", "de-de"),
				EnsureQueryParam(1, "sr", "390x844"),
				setClientID(2, "client-b"),
				setDeviceProfile(2, deviceProfile{
					userAgent:          deviceGeoSessionDesktopUA,
					clientHintPlatform: deviceGeoSessionDesktopCH,
					queryParamPlatform: "Linux",
				}),
				EnsureQueryParam(2, "ul", "es-es"),
				EnsureQueryParam(2, "sr", "2560x1440"),
			},
			expectations: map[string]any{
				"session_geo_city":                        "Paris",
				"session_geo_region":                      "Ile-de-France",
				"session_geo_metro":                       nil,
				"session_geo_country":                     "France",
				"session_geo_continent":                   "Europe",
				"session_geo_sub_continent":               nil,
				"session_device_category":                 "smartphone",
				"session_device_language":                 "de-de",
				"session_device_mobile_brand_name":        "Apple",
				"session_device_mobile_model_name":        "iPhone",
				"session_device_operating_system":         "iOS",
				"session_device_operating_system_version": "11.0",
				"session_device_web_browser":              "Mobile Safari",
				"session_device_web_browser_version":      "11.0",
				"session_device_screen_resolution":        "390x844",
			},
		},
		{
			name: "missing selected event field propagates nil",
			hits: TestHits{TestHitOne()},
			config: []CaseConfigFunc{
				SetColumnsRegistry(registry),
				setClientID(0, "client-a"),
				setDeviceProfile(0, deviceProfile{
					userAgent:          deviceGeoSessionIPhoneUA,
					queryParamPlatform: "iOS",
				}),
				EnsureQueryParam(0, "ul", "it-it"),
				deleteQueryParam(0, "sr"),
			},
			expectations: map[string]any{
				"session_device_screen_resolution": nil,
				"session_device_web_browser":       "Mobile Safari",
				"session_geo_country":              "France",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ColumnTestCase(t, tc.hits, func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
				require.NoError(t, closeErr)
				require.NotEmpty(t, whd.WriteCalls)
				require.NotEmpty(t, whd.WriteCalls[0].Records)

				record := whd.WriteCalls[0].Records[0]
				for fieldName, expected := range tc.expectations {
					assert.Equal(t, expected, record[fieldName], fieldName)
				}
			}, proto, tc.config...)
		})
	}
}

func setClientID(hitNum int, clientID string) CaseConfigFunc {
	return func(t *testing.T, c *CaseConfig) {
		c.hits[hitNum].ClientID = hits.ClientID(clientID)
		c.hits[hitNum].AuthoritativeClientID = hits.ClientID(clientID)
		c.hits[hitNum].MustParsedRequest().QueryParams.Set("cid", clientID)
	}
}

func deleteQueryParam(hitNum int, param string) CaseConfigFunc {
	return func(t *testing.T, c *CaseConfig) {
		c.hits[hitNum].MustParsedRequest().QueryParams.Del(param)
	}
}

type deviceProfile struct {
	userAgent          string
	clientHintPlatform string
	queryParamPlatform string
}

func setDeviceProfile(hitNum int, profile deviceProfile) CaseConfigFunc {
	return func(t *testing.T, c *CaseConfig) {
		req := c.hits[hitNum].MustParsedRequest()
		req.Headers.Del("User-Agent")
		delete(req.Headers, "user-agent")
		req.Headers.Set("User-Agent", profile.userAgent)
		if profile.clientHintPlatform == "" {
			req.Headers.Del("sec-ch-ua")
			req.Headers.Del("sec-ch-ua-mobile")
			req.Headers.Del("sec-ch-ua-platform")
			delete(req.Headers, "sec-ch-ua")
			delete(req.Headers, "sec-ch-ua-mobile")
			delete(req.Headers, "sec-ch-ua-platform")
		} else {
			req.Headers.Del("sec-ch-ua-platform")
			delete(req.Headers, "sec-ch-ua-platform")
			req.Headers.Set("sec-ch-ua-platform", profile.clientHintPlatform)
		}
		req.QueryParams.Set("uap", profile.queryParamPlatform)
		req.QueryParams.Del("uapv")
	}
}
