package properties

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTransformationsConfig(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		rules      int
		wantError  bool
	}{
		{"absent", "unrelated: true", 0, false},
		{"valid", `
unrelated: true
transformations:
  rules:
    - name: example
      expression: 'true'
      actions:
        - type: set
          target: {parameter: score, type: number}
          value: {number: 0}
`, 1, false},
		{"typo", `transformations: {rules: [{expression: 'true', actions: [{type: set, targte: {field: name}}]}]}`, 0, true},
		{"unknown section field", `transformations: {rulse: []}`, 0, true},
		{"malformed", `transformations: [`, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.yaml), 0o600))
			// when
			config, err := ParseTransformationsConfig(path)
			// then
			assert.Equal(t, tc.wantError, err != nil)
			if !tc.wantError {
				assert.Len(t, config.Rules, tc.rules)
			}
		})
	}
}
