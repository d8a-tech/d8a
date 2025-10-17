package dbip

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
