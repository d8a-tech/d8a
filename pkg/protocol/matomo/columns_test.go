package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/protocol"
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
		NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"}),
	)
}

type staticPropertyIDExtractor struct {
	propertyID string
}

func (e *staticPropertyIDExtractor) PropertyID(_ *protocol.RequestContext) (string, error) {
	return e.propertyID, nil
}
