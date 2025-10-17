package dbip

import (
	"context"
	"net/netip"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

type resultCityNames struct {
	English string `maxminddb:"en"`
}

type resultCity struct {
	Names resultCityNames `maxminddb:"names"`
}

type resultContinentNames struct {
	English string `maxminddb:"en"`
}

type resultContinent struct {
	Code      string               `maxminddb:"code"`
	GeonameID uint32               `maxminddb:"geoname_id"`
	Names     resultContinentNames `maxminddb:"names"`
}

type resultCountryNames struct {
	English string `maxminddb:"en"`
}

type resultCountry struct {
	GeonameID         uint32             `maxminddb:"geoname_id"`
	IsInEuropeanUnion bool               `maxminddb:"is_in_european_union"`
	ISOCode           string             `maxminddb:"iso_code"`
	Names             resultCountryNames `maxminddb:"names"`
}

type resultLocation struct {
	Latitude  float64 `maxminddb:"latitude"`
	Longitude float64 `maxminddb:"longitude"`
}

type resultSubdivisionNames struct {
	English string `maxminddb:"en"`
}

type resultSubdivision struct {
	Names resultSubdivisionNames `maxminddb:"names"`
}

type result struct {
	City         resultCity          `maxminddb:"city"`
	Continent    resultContinent     `maxminddb:"continent"`
	Country      resultCountry       `maxminddb:"country"`
	Location     resultLocation      `maxminddb:"location"`
	Subdivisions []resultSubdivision `maxminddb:"subdivisions"`
}

const geoRecordMetadataKey = "geo_record"

func geoColumn(
	mmdbPath string,
	column schema.InterfaceID,
	field *arrow.Field,
	getValue func(event *schema.Event, record *result) (any, error),
) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		column,
		field,
		func(event *schema.Event) (any, error) {
			cachedRecord, ok := event.Metadata[geoRecordMetadataKey]
			if ok {
				typedCachedRecord, ok := cachedRecord.(*result)
				if ok {
					return getValue(event, typedCachedRecord)
				}
			}
			ipAddress := netip.MustParseAddr(event.BoundHit.IP)
			db, err := GetMMDB(mmdbPath)
			if err != nil {
				return nil, err
			}
			var record result
			err = db.Lookup(ipAddress).Decode(&record)
			if err != nil {
				return nil, err
			}
			event.Metadata[geoRecordMetadataKey] = &record
			return getValue(event, &record)
		},
	)
}

// CityColumn creates a city column from a MMDB path.
func CityColumn(mmdbPath string) schema.EventColumn {
	return geoColumn(
		mmdbPath,
		columns.CoreInterfaces.GeoCity.ID,
		columns.CoreInterfaces.GeoCity.Field,
		func(_ *schema.Event, record *result) (any, error) {
			return record.City.Names.English, nil
		},
	)
}

// CountryColumn creates a country column from a MMDB path.
func CountryColumn(mmdbPath string) schema.EventColumn {
	return geoColumn(
		mmdbPath,
		columns.CoreInterfaces.GeoCountry.ID,
		columns.CoreInterfaces.GeoCountry.Field,
		func(_ *schema.Event, record *result) (any, error) {
			return record.Country.Names.English, nil
		},
	)
}

// ContinentColumn creates a continent column from a MMDB path.
func ContinentColumn(mmdbPath string) schema.EventColumn {
	return geoColumn(
		mmdbPath,
		columns.CoreInterfaces.GeoContinent.ID,
		columns.CoreInterfaces.GeoContinent.Field,
		func(_ *schema.Event, record *result) (any, error) {
			return record.Continent.Names.English, nil
		},
	)
}

// RegionColumn creates a region column from a MMDB path.
func RegionColumn(mmdbPath string) schema.EventColumn {
	return geoColumn(
		mmdbPath,
		columns.CoreInterfaces.GeoRegion.ID,
		columns.CoreInterfaces.GeoRegion.Field,
		func(_ *schema.Event, record *result) (any, error) {
			if len(record.Subdivisions) == 0 {
				return nil, nil //nolint:nilnil // nil is valid
			}
			return record.Subdivisions[0].Names.English, nil
		},
	)
}

// GeoColumns creates a set of geo columns from a downloader.
func GeoColumns(downloader MMDBCityDatabaseDownloader, destinationDirectory string) []schema.EventColumn {
	if destinationDirectory == "" {
		destinationDirectory = "/tmp"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mmdbPath, err := downloader.Download(
		ctx,
		"dbip-city-lite",
		"latest",
		destinationDirectory,
	)
	if err != nil {
		logrus.WithError(err).Panic("failed to download MMDB city database")
	}
	return []schema.EventColumn{
		ContinentColumn(mmdbPath),
		CityColumn(mmdbPath),
		CountryColumn(mmdbPath),
		RegionColumn(mmdbPath),
		eventcolumns.GeoSubContinentStubColumn,
		eventcolumns.GeoMetroStubColumn,
	}
}
