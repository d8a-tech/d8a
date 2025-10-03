package currency

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBankersRound(t *testing.T) {
	tests := []struct {
		name      string
		value     float64
		precision int
		expected  float64
	}{
		{
			name:      "round half to even - 2.5",
			value:     2.5,
			precision: 0,
			expected:  2.0,
		},
		{
			name:      "round half to even - 3.5",
			value:     3.5,
			precision: 0,
			expected:  4.0,
		},
		{
			name:      "round half to even - 2.25",
			value:     2.25,
			precision: 1,
			expected:  2.2,
		},
		{
			name:      "round half to even - 2.35",
			value:     2.35,
			precision: 1,
			expected:  2.4,
		},
		{
			name:      "standard rounding - 2.6",
			value:     2.6,
			precision: 0,
			expected:  3.0,
		},
		{
			name:      "standard rounding - 2.4",
			value:     2.4,
			precision: 0,
			expected:  2.0,
		},
		{
			name:      "currency precision - 2 decimal places",
			value:     1.235,
			precision: 2,
			expected:  1.24,
		},
		{
			name:      "currency precision - round half to even",
			value:     1.125,
			precision: 2,
			expected:  1.12,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			value := tt.value
			precision := tt.precision

			// when
			result := bankersRound(value, precision)

			// then
			assert.Equal(t, tt.expected, result)
		})
	}
}
