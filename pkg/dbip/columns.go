// Package dbip provides columns for the DBIP database.
package dbip

import (
	"context"
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

// GeoColumnFactory is a template for creating geo columns.
type GeoColumnFactory struct {
	mmdbPath    string
	cache       *ristretto.Cache[string, *result]
	cacheConfig CacheConfig
}

// NewGeoColumnFactory creates a new GeoColumnTemplate.
func NewGeoColumnFactory(mmdbPath string, cacheConfig CacheConfig) (*GeoColumnFactory, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, *result]{
		NumCounters: cacheConfig.MaxEntries * 10,
		MaxCost:     cacheConfig.MaxEntries,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &GeoColumnFactory{
		mmdbPath:    mmdbPath,
		cache:       cache,
		cacheConfig: cacheConfig,
	}, nil
}

const geoRecordMetadataKey = "geo_record"

// Column creates a new event column from a GeoColumnTemplate.
func (t *GeoColumnFactory) Column(
	column schema.InterfaceID,
	field *arrow.Field,
	getValue func(event *schema.Event, record *result) (any, schema.D8AColumnWriteError),
	options ...columns.EventColumnOptions,
) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		column,
		field,
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			// Check if the value was computed for other column in this event
			computedRecord, ok := event.Metadata[geoRecordMetadataKey]
			if ok {
				typedComputedRecord, ok := computedRecord.(*result)
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
			db, err := GetMaxmindReader(t.mmdbPath)
			if err != nil {
				return nil, schema.NewRetryableError(fmt.Sprintf("failed to get maxmind reader: %s", err))
			}
			var record result
			err = db.Lookup(ipAddress).Decode(&record)
			if err != nil {
				return nil, schema.NewBrokenEventError(fmt.Sprintf("failed to lookup IP address in dbip column: %s", err))
			}
			event.Metadata[geoRecordMetadataKey] = &record
			t.cache.SetWithTTL(event.BoundHit.MustParsedRequest().IP, &record, 1, t.cacheConfig.TTL)
			return getValue(event, &record)
		},
		options...,
	)
}

// CityColumn creates a city column from a MMDB path.
func CityColumn(t *GeoColumnFactory) schema.EventColumn {
	return t.Column(
		columns.CoreInterfaces.GeoCity.ID,
		columns.CoreInterfaces.GeoCity.Field,
		func(_ *schema.Event, record *result) (any, schema.D8AColumnWriteError) {
			return record.City.Names.English, nil
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
		func(_ *schema.Event, record *result) (any, schema.D8AColumnWriteError) {
			return record.Country.Names.English, nil
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
		func(_ *schema.Event, record *result) (any, schema.D8AColumnWriteError) {
			return record.Continent.Names.English, nil
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
		func(_ *schema.Event, record *result) (any, schema.D8AColumnWriteError) {
			if len(record.Subdivisions) == 0 {
				return nil, nil //nolint:nilnil // nil is valid
			}
			return record.Subdivisions[0].Names.English, nil
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

// GeoColumns creates a set of geo columns from a downloader.
func GeoColumns(
	downloader Downloader,
	destinationDirectory string,
	downloadTimeout time.Duration,
	cacheConfig CacheConfig,
) []schema.EventColumn {
	if destinationDirectory == "" {
		destinationDirectory = "/tmp"
	}

	// Check for existing local MMDB files before attempting download.
	bestLocalMMDBPath, err := selectBestMMDBFile(destinationDirectory, ".mmdb")
	if err != nil {
		logrus.WithError(err).Warn("failed to scan for existing MMDB files, proceeding with download attempt")
		bestLocalMMDBPath = ""
	}

	// Attempt to download/check for new version
	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()
	mmdbPath, err := downloader.Download(
		ctx,
		"dbip-city-lite",
		"latest",
		destinationDirectory,
	)
	if err != nil {
		// If download/check failed but we have a local file, warn and use it
		if bestLocalMMDBPath != "" {
			logrus.WithError(err).WithFields(logrus.Fields{
				"fallback_path": bestLocalMMDBPath,
			}).Warn("failed to download/check MMDB city database, using existing local file")
			mmdbPath = bestLocalMMDBPath
		} else {
			// No local file and download failed - fail startup
			logrus.WithError(err).Panic("failed to download MMDB city database and no existing local file found")
		}
	}

	t, err := NewGeoColumnFactory(mmdbPath, cacheConfig)
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
