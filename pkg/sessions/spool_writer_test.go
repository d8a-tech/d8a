package sessions

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirectSpoolWriterWrite_SanitizesPropertyIDPathSeparators(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	writer, err := NewDirectSpoolWriter(tmpDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		errClose := writer.Close()
		require.NoError(t, errClose)
	})

	propID := "../tenant\\prod"
	payload := []byte("payload")

	// when
	err = writer.Write(propID, payload)
	require.NoError(t, err)

	// then
	expectedSpoolPath := activeSpoolPath(tmpDir, propID)
	_, err = os.Stat(expectedSpoolPath)
	require.NoError(t, err)

	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "property_.._tenant_prod.spool", entries[0].Name())
}
