package monitoring

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteBuckets(t *testing.T) {
	assert.Equal(t, []float64{
		1 << 10,
		2 << 10,
		4 << 10,
		8 << 10,
		16 << 10,
		32 << 10,
		64 << 10,
		128 << 10,
		256 << 10,
		512 << 10,
		1 << 20,
		2 << 20,
		4 << 20,
		8 << 20,
		16 << 20,
		32 << 20,
		64 << 20,
		128 << 20,
		256 << 20,
		512 << 20,
	}, ByteBuckets)
}
