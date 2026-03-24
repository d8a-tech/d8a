package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLocalfetchURL(t *testing.T) {
	t.Run("normalizes path and preserves query string", func(t *testing.T) {
		url, err := buildLocalfetchURL(17041, "g/collect", "a=1&b=two")

		require.NoError(t, err)
		assert.Equal(t, "http://localhost:17041/g/collect?a=1&b=two", url)
	})

	t.Run("rejects invalid query string", func(t *testing.T) {
		_, err := buildLocalfetchURL(17041, "/g/collect", "%zz")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid query string")
	})
}
