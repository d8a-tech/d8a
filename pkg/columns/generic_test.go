package columns

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
)

func TestCastToBool(t *testing.T) {
	// given
	columnID := schema.InterfaceID("test_column")
	castFunc := CastToBool(columnID)

	tests := []struct {
		name      string
		input     any
		expected  bool
		expectErr bool
	}{
		// Boolean inputs
		{"direct true", true, true, false},
		{"direct false", false, false, false},

		// Truthy string values
		{"string true", "true", true, false},
		{"string TRUE", "TRUE", true, false},
		{"string yes", "yes", true, false},
		{"string YES", "YES", true, false},
		{"string y", "y", true, false},
		{"string Y", "Y", true, false},
		{"string on", "on", true, false},
		{"string ON", "ON", true, false},
		{"string 1", "1", true, false},
		{"string t", "t", true, false},
		{"string T", "T", true, false},

		// Falsy string values
		{"string false", "false", false, false},
		{"string FALSE", "FALSE", false, false},
		{"string no", "no", false, false},
		{"string NO", "NO", false, false},
		{"string n", "n", false, false},
		{"string N", "N", false, false},
		{"string off", "off", false, false},
		{"string OFF", "OFF", false, false},
		{"string 0", "0", false, false},
		{"string f", "f", false, false},
		{"string F", "F", false, false},
		{"empty string", "", false, true},

		// Whitespace handling
		{"whitespace true", "  true  ", true, false},
		{"whitespace false", "  false  ", false, false},
		{"whitespace yes", "\tyes\n", true, false},

		// Unrecognized values (should return error)
		{"unrecognized string", "maybe", false, true},
		{"number 2", "2", false, true},
		{"random text", "random", false, true},

		// Non-string, non-boolean values (should return false)
		{"integer 1", 1, false, false},
		{"integer 0", 0, false, false},
		{"nil value", nil, false, false},
		{"slice", []string{"test"}, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			result, err := castFunc(tt.input)

			// then
			if tt.expectErr {
				assert.Error(t, err)
				assert.Equal(t, tt.expected, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
