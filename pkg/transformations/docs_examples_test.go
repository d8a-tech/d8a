package transformations

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every YAML example in the user docs must parse and compile, so the docs can't drift.
func TestDocumentationExamplesCompile(t *testing.T) {
	// given
	content, err := os.ReadFile(filepath.Join("..", "..", "docs", "docs", "articles", "event-transformations.md"))
	require.NoError(t, err)
	blocks := regexp.MustCompile("(?s)```yaml\n(.*?)```").FindAllStringSubmatch(string(content), -1)
	require.Greater(t, len(blocks), 15)
	for i, block := range blocks {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(block[1]), 0o600))
		// when
		config, err := properties.ParseTransformationsConfig(path)
		require.NoError(t, err, "example %d", i+1)
		_, err = Compile(config)
		// then
		assert.NoError(t, err, "example %d:\n%s", i+1, block[1])
		assert.NotEmpty(t, config.Rules, "example %d", i+1)
	}
}
