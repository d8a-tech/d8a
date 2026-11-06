package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestGenerateConfigDocsEscapesMDXBraces(t *testing.T) {
	// given a flag whose description contains placeholders in braces
	flag := &cli.StringFlag{Name: "example-url", Usage: "Must serve {base}/{currency}.json."}

	// when
	output, err := generateConfigDocs([]cli.Flag{flag})

	// then the braces are escaped so MDX renders them as text
	require.NoError(t, err)
	assert.Contains(t, output, `Must serve \{base\}/\{currency\}.json.`)
}
