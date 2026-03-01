package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/require"
)

func TestAllColumns(t *testing.T) {
	columntests.ColumnTestCase(
		t,
		columntests.TestHits{columntests.TestHitOne(), columntests.TestHitTwo()},
		func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
			require.NoError(t, closeErr)
		},
		NewMatomoProtocol(),
	)
}
