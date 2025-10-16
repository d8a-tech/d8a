package dbipe2e

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionPageNumber(t *testing.T) {
	columntests.ColumnTestCase(
		t,
		columntests.TestHits{columntests.TestHitOne()},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			// when + then
			require.NoError(t, closeErr)
			assert.Equal(t, "a", whd.WriteCalls[0].Records[0]["session_page_number"])
			assert.Equal(t, int64(0), whd.WriteCalls[0].Records[1]["session_page_number"])
			assert.Equal(t, int64(1), whd.WriteCalls[0].Records[2]["session_page_number"])
		},
		ga4.NewGA4Protocol(currency.NewDummyConverter(1)),
	)
}
