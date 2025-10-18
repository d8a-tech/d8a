package dbip

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBIPColumns(t *testing.T) {
	t.Skip("Skipping test as it has external dependencies - run it only on demand")
	th1 := columntests.TestHitOne()
	th1.IP = "80.68.239.25"
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
		ga4.NewGA4Protocol(currency.NewDummyConverter(1)),
		columntests.SetColumnsRegistry(
			columnset.DefaultColumnRegistry(
				ga4.NewGA4Protocol(
					currency.NewDummyConverter(1),
				),
				GeoColumns(
					NewOnlyOnceDownloader(
						NewExtensionBasedOCIDownloader(
							OCIRegistryCreds{
								Repo:       "ghcr.io/d8a-tech",
								IgnoreCert: false,
							},
							".mmdb",
						),
					),
					"/tmp",
					60*time.Second,
					CacheConfig{
						MaxCost: 2137,
						TTL:     30 * time.Second,
					},
				),
			),
		),
	)
}
