package files

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/warehouse"
)

// TestSpoolDriverImplementsDriver verifies that spoolDriver implements warehouse.Driver
func TestSpoolDriverImplementsDriver(t *testing.T) {
	var _ warehouse.Driver = (*spoolDriver)(nil)
}
