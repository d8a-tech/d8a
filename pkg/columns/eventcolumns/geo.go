package eventcolumns

import "github.com/d8a-tech/d8a/pkg/columns"

var stubDescription = "Stub column, contains null. Your geolocation provider does not implement this yet." // nolint:lll // it's a description

// GeoContinentStubColumn is the column for the geo continent of an event
var GeoContinentStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.GeoContinent.ID,
	columns.CoreInterfaces.GeoContinent.Field,
	columns.WithEventColumnDocs(
		"Continent",
		stubDescription,
	),
)

// GeoCountryStubColumn is the column for the geo country of an event
var GeoCountryStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.GeoCountry.ID,
	columns.CoreInterfaces.GeoCountry.Field,
	columns.WithEventColumnDocs(
		"Country",
		stubDescription,
	),
)

// GeoRegionStubColumn is the column for the geo region of an event
var GeoRegionStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.GeoRegion.ID,
	columns.CoreInterfaces.GeoRegion.Field,
	columns.WithEventColumnDocs(
		"Region",
		stubDescription,
	),
)

// GeoCityStubColumn is the column for the geo city of an event
var GeoCityStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.GeoCity.ID,
	columns.CoreInterfaces.GeoCity.Field,
	columns.WithEventColumnDocs(
		"City",
		stubDescription,
	),
)

// GeoSubContinentStubColumn is the column for the geo subcontinent of an event
var GeoSubContinentStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.GeoSubContinent.ID,
	columns.CoreInterfaces.GeoSubContinent.Field,
	columns.WithEventColumnDocs(
		"Sub-Continent",
		stubDescription,
	),
)

// GeoMetroStubColumn is the column for the geo metro of an event
var GeoMetroStubColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.GeoMetro.ID,
	columns.CoreInterfaces.GeoMetro.Field,
	columns.WithEventColumnDocs(
		"Metro Area",
		stubDescription,
	),
)
