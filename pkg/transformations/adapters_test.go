package transformations

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/d8a"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/protocol/matomo"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtocolColumnExtractionParity(t *testing.T) {
	// given
	settings := &properties.Settings{ExcludedURLParams: []string{"secret"}}
	registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(settings))
	converter := currency.NewDummyConverter(1)
	for _, implementation := range []protocol.Protocol{
		ga4.NewGA4Protocol(converter, registry), d8a.NewD8AProtocol(converter, registry),
		matomo.NewMatomoProtocol(matomo.NewFromIDSiteExtractor(registry), registry),
	} {
		t.Run(implementation.ID(), func(t *testing.T) {
			hit := hits.New()
			hit.EventName = "page_view"
			view := newEventView(implementation.ID(), hit, settings, ExtractionDependencies{})
			for name, value := range map[string]string{
				"page_location": "https://example.com/a%20b?secret=canary&keep=yes",
				"page_title":    "hello", "page_referrer": "https://referrer.example/",
			} {
				hit.Request.QueryParams.Set(view.wireKey(ref{Field: name}), value)
			}
			event := schema.NewEvent(hit)
			for _, name := range []string{"page_location", "page_title", "page_referrer", "page_hostname", "page_path"} {
				// when
				value, err := view.get(ref{Field: name})
				require.NoError(t, err)
				found := false
				for _, column := range implementation.Columns().Event {
					if column.Implements().Field.Name == name {
						found = true
						require.NoError(t, column.Write(event))
					}
				}
				// then
				require.True(t, found, name)
				assert.Equal(t, event.Values[name], value, name)
			}
		})
	}
}

func TestDeviceAndGeoColumnExtractionParity(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.IP = "192.168.1.0"
	hit.Request.Headers.Set("User-Agent",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")
	hit.Request.Headers.Set("Sec-Ch-Ua-Platform", `"Linux"`)
	geo, err := dbip.NewGeoColumnFactory(&geoStub{record: &dbip.LookupResult{
		Country: "NL", Region: "Noord-Holland", City: "Amsterdam",
	}}, dbip.CacheConfig{MaxEntries: 10, TTL: time.Minute})
	require.NoError(t, err)
	view := newEventView("ga4", hit, nil, ExtractionDependencies{Geo: geo})
	event := schema.NewEvent(hit)
	for _, column := range []schema.EventColumn{
		eventcolumns.DeviceWebBrowserColumn, eventcolumns.DeviceOperatingSystemColumn, eventcolumns.DeviceCategoryColumn,
		dbip.CountryColumn(geo), dbip.RegionColumn(geo), dbip.CityColumn(geo),
	} {
		// when
		name := column.Implements().Field.Name
		value, err := view.get(ref{Field: name})
		require.NoError(t, err)
		require.NoError(t, column.Write(event))
		// then
		assert.Equal(t, event.Values[name], value, name)
	}
}
