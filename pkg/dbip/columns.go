// Package dbip provides columns for the DBIP database.
package dbip

import (
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/sirupsen/logrus"
)

// LookupResult stores normalized geolocation values extracted from DBIP.
type LookupResult struct {
	City      string
	Country   string
	Continent string
	Region    string
}

// LookupProvider returns geolocation values for IP addresses.
type LookupProvider interface {
	Lookup(ip netip.Addr) (*LookupResult, error)
}

// GeoColumnFactory is a template for creating geo columns.
type GeoColumnFactory struct {
	provider    LookupProvider
	cache       *ristretto.Cache[string, *LookupResult]
	cacheConfig CacheConfig
}

// NewGeoColumnFactory creates a new GeoColumnTemplate.
func NewGeoColumnFactory(provider LookupProvider, cacheConfig CacheConfig) (*GeoColumnFactory, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, *LookupResult]{
		NumCounters: cacheConfig.MaxEntries * 10,
		MaxCost:     cacheConfig.MaxEntries,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	if provider == nil {
		provider = NewUnavailableLookupProvider()
	}
	return &GeoColumnFactory{
		provider:    provider,
		cache:       cache,
		cacheConfig: cacheConfig,
	}, nil
}

const geoRecordMetadataKey = "geo_record"

// Column creates a new event column from a GeoColumnTemplate.
func (t *GeoColumnFactory) Column(
	column schema.InterfaceID,
	field *arrow.Field,
	getValue func(event *schema.Event, record *LookupResult) (any, schema.D8AColumnWriteError),
	options ...columns.EventColumnOptions,
) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		column,
		field,
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			// Check if the value was computed for other column in this event
			computedRecord, ok := event.Metadata[geoRecordMetadataKey]
			if ok {
				typedComputedRecord, ok := computedRecord.(*LookupResult)
				if ok {
					return getValue(event, typedComputedRecord)
				}
			}
			// Check if for given IP there is a cache hit (calculated for other event)
			cacheHit, ok := t.cache.Get(event.BoundHit.MustParsedRequest().IP)
			if ok {
				event.Metadata[geoRecordMetadataKey] = cacheHit
				return getValue(event, cacheHit)
			}
			ipAddress, err := netip.ParseAddr(event.BoundHit.MustParsedRequest().IP)
			if err != nil {
				logrus.WithError(err).Warn("failed to parse IP address in dbip column")
				return nil, nil //nolint:nilnil // nil is valid
			}
			record, err := t.provider.Lookup(ipAddress)
			if err != nil {
				if errors.Is(err, ErrUnavailable) {
					return nil, nil //nolint:nilnil // nil is valid
				}
				return nil, schema.NewBrokenEventError(fmt.Sprintf("failed to lookup IP address in dbip column: %s", err))
			}
			event.Metadata[geoRecordMetadataKey] = record
			t.cache.SetWithTTL(event.BoundHit.MustParsedRequest().IP, record, 1, t.cacheConfig.TTL)
			return getValue(event, record)
		},
		options...,
	)
}

// CityColumn creates a city column from a MMDB path.
func CityColumn(t *GeoColumnFactory) schema.EventColumn {
	return t.Column(
		columns.CoreInterfaces.GeoCity.ID,
		columns.CoreInterfaces.GeoCity.Field,
		func(_ *schema.Event, record *LookupResult) (any, schema.D8AColumnWriteError) {
			return record.City, nil
		},
		columns.WithEventColumnDocs(
			"City (Provided by DBIP)",
			"Geolocated city name (e.g., 'New York', 'London').",
		),
	)
}

// CountryColumn creates a country column from a MMDB path.
func CountryColumn(t *GeoColumnFactory) schema.EventColumn {
	return t.Column(
		columns.CoreInterfaces.GeoCountry.ID,
		columns.CoreInterfaces.GeoCountry.Field,
		func(_ *schema.Event, record *LookupResult) (any, schema.D8AColumnWriteError) {
			return record.Country, nil
		},
		columns.WithEventColumnDocs(
			"Country (Provided by DBIP)",
			"Geolocated country name (e.g., 'United States', 'United Kingdom').",
		),
	)
}

// ContinentColumn creates a continent column from a MMDB path.
func ContinentColumn(t *GeoColumnFactory) schema.EventColumn {
	return t.Column(
		columns.CoreInterfaces.GeoContinent.ID,
		columns.CoreInterfaces.GeoContinent.Field,
		func(_ *schema.Event, record *LookupResult) (any, schema.D8AColumnWriteError) {
			return record.Continent, nil
		},
		columns.WithEventColumnDocs(
			"Continent (Provided by DBIP)",
			"Geolocated continent name (e.g., 'Europe', 'North America').",
		),
	)
}

// RegionColumn creates a region column from a MMDB path.
func RegionColumn(t *GeoColumnFactory) schema.EventColumn {
	return t.Column(
		columns.CoreInterfaces.GeoRegion.ID,
		columns.CoreInterfaces.GeoRegion.Field,
		func(_ *schema.Event, record *LookupResult) (any, schema.D8AColumnWriteError) {
			if record.Region == "" {
				return nil, nil //nolint:nilnil // nil is valid
			}
			return record.Region, nil
		},
		columns.WithEventColumnDocs(
			"Region (Provided by DBIP)",
			"Geolocated region or state name (e.g., 'California', 'England').",
		),
	)
}

// CacheConfig is the configuration for the cache.
type CacheConfig struct {
	MaxEntries int64
	TTL        time.Duration
}

// GeoColumns creates a set of geo columns backed by the provided lookup provider.
func GeoColumns(provider LookupProvider, cacheConfig CacheConfig) []schema.EventColumn {
	t, err := NewGeoColumnFactory(provider, cacheConfig)
	if err != nil {
		logrus.WithError(err).Panic("failed to create geo column template")
	}
	return []schema.EventColumn{
		ContinentColumn(t),
		CityColumn(t),
		CountryColumn(t),
		RegionColumn(t),
		eventcolumns.GeoSubContinentStubColumn,
		eventcolumns.GeoMetroStubColumn,
	}
}
