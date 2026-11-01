package transformations

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every catalog field must read exactly what the worker stores for the same hit.
func TestCatalogReadsMatchWorkerColumns(t *testing.T) {
	// given
	settings := &properties.Settings{ExcludedURLParams: []string{"secret"}}
	deps := columnDeps(settings, &geoStub{record: &dbip.LookupResult{
		Country: "NL", Region: "Noord-Holland", City: "Amsterdam", Continent: "Europe",
	}})
	for protocolID, query := range map[string]map[string]string{
		"ga4": {"dl": "https://example.com/a%20b?secret=canary&utm_source=news&gclid=1", "dt": "hello",
			"dr": "https://referrer.example/", "sr": "1920x1080", "ul": "en-us", "uid": "user", "en": "page_view",
			"tid": "G-TEST"},
		"matomo": {"url": "https://example.com/a?utm_medium=cpc", "action_name": "hello", "res": "1920x1080",
			"urlref": "https://referrer.example/", "uid": "user", "e_c": "category", "revenue": "12.5", "idsite": "1"},
	} {
		t.Run(protocolID, func(t *testing.T) {
			hit := hits.New()
			hit.EventName = "page_view"
			hit.Request.IP = "192.168.1.0"
			for key, value := range query {
				hit.Request.QueryParams.Set(key, value)
			}
			cols, err := deps.Columns[protocolID].Get(hit.PropertyID)
			require.NoError(t, err)
			event := schema.NewEvent(hit)
			for _, column := range cols.Event {
				require.NoError(t, column.Write(event), column.Implements().Field.Name)
			}
			checked := 0
			for _, capability := range testCatalog.Fields() {
				if capability.ConditionOnly || capability.Field == "parameter" || capability.Field == "url_parameter" ||
					!contains(capability.ReadProtocols, protocolID) {
					continue
				}
				// when
				view := newEventView(Input{Protocol: protocolID, Hit: hit, Settings: settings, Dependencies: deps}, testCatalog)
				value, err := view.field(capability.Field)
				// then
				require.NoError(t, err, capability.Field)
				assert.Equal(t, normalizeValue(event.Values[capability.Field]), value, capability.Field)
				checked++
			}
			assert.Greater(t, checked, 20)
		})
	}
}

func TestCatalogExcludesDeviceDetectionAndExposesKnownParameters(t *testing.T) {
	// given
	fields := make(map[string]FieldCapability)
	for _, capability := range testCatalog.Fields() {
		fields[capability.Field] = capability
	}
	// then
	for _, name := range []string{"device_category", "device_web_browser", "device_operating_system", "id", "client_id"} {
		assert.NotContains(t, fields, name)
	}
	assert.Contains(t, fields, "device_screen_resolution")
	assert.True(t, fields[IPAddressField].ConditionOnly)
	assert.Empty(t, fields[IPAddressField].WriteProtocols)
	assert.True(t, fields[PropertyIDField].ConditionOnly)
	assert.Equal(t, []string{"==", "!=", "in"}, fields[PropertyIDField].Operators)
	assert.NotContains(t, fields, "property_name")
	assert.Equal(t, []string{"ga4", "d8a", "matomo"}, fields["geo_country"].WriteProtocols)
	assert.Equal(t, []string{"ga4", "d8a"}, fields["name"].WriteProtocols)
	assert.False(t, fields["name"].Removable)
	assert.Equal(t, CategoryTrafficSource, fields["utm_source"].Category)
	assert.Contains(t, testCatalog.KnownParameters(), ParameterHint{Name: "percent_scrolled", Type: "number"})
	for _, hint := range testCatalog.KnownParameters() {
		if field, ok := fields["params_"+hint.Name]; ok {
			assert.NotContains(t, field.ReadProtocols, "ga4", "known GA4 parameters are hints, not fields")
		}
	}
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}
