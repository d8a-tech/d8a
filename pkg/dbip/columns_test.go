package dbip_test

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBIPColumns(t *testing.T) {
	th1 := columntests.TestHitOne()
	th1.Request.IP = "80.68.239.25"
	columntests.ColumnTestCase(
		t,
		columntests.TestHits{th1},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			assert.Equal(t, "Wroclaw", whd.WriteCalls[0].Records[0]["geo_city"])
			assert.Equal(t, "Poland", whd.WriteCalls[0].Records[0]["geo_country"])
			assert.Equal(t, "Europe", whd.WriteCalls[0].Records[0]["geo_continent"])
			assert.Equal(t, "Lower Silesia", whd.WriteCalls[0].Records[0]["geo_region"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
		columntests.SetColumnsRegistry(
			columnset.DefaultColumnRegistry(
				ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
				properties.NewTestSettingRegistry(),
				columnset.WithGeoProvider(
					dbip.NewStaticLookupProvider(&dbip.LookupResult{
						City:      "Wroclaw",
						Country:   "Poland",
						Continent: "Europe",
						Region:    "Lower Silesia",
					}, nil),
				),
			),
		),
	)
}

func TestDBIPColumns_WhenUnavailable_WritesNulls(t *testing.T) {
	th1 := columntests.TestHitOne()
	th1.Request.IP = "80.68.239.25"
	columntests.ColumnTestCase(
		t,
		columntests.TestHits{th1},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			require.NoError(t, closeErr)
			assert.Nil(t, whd.WriteCalls[0].Records[0]["geo_city"])
			assert.Nil(t, whd.WriteCalls[0].Records[0]["geo_country"])
			assert.Nil(t, whd.WriteCalls[0].Records[0]["geo_continent"])
			assert.Nil(t, whd.WriteCalls[0].Records[0]["geo_region"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
		columntests.SetColumnsRegistry(
			columnset.DefaultColumnRegistry(
				ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
				properties.NewTestSettingRegistry(),
				columnset.WithGeoProvider(
					dbip.NewStaticLookupProvider(nil, nil),
				),
			),
		),
	)
}
