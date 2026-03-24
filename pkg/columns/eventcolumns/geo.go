package eventcolumns

import "github.com/d8a-tech/d8a/pkg/columns"

var stubDescription = "Stub column, always returns null, since your geolocation provider does not implement this column." // nolint:lll // it's a description

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
