package files

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilesystemUploader_Upload_Success(t *testing.T) {
	// given
	srcDir := t.TempDir()
	destDir := t.TempDir()

	// Create a test file in the source directory
	testFilename := "test.txt"
	srcFilePath := filepath.Join(srcDir, testFilename)
	testContent := []byte("test data")
	err := os.WriteFile(srcFilePath, testContent, 0o644)
	assert.NoError(t, err)

	uploader, err := NewFilesystemUploader(destDir)
	assert.NoError(t, err)
	assert.NotNil(t, uploader)

	// when
	err = uploader.Upload(context.Background(), srcFilePath)

	// then
	assert.NoError(t, err)
	// Verify the file was moved (no longer exists at source)
	_, err = os.Stat(srcFilePath)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
	// Verify the file exists at destination
	destFilePath := filepath.Join(destDir, testFilename)
	data, err := os.ReadFile(destFilePath)
	assert.NoError(t, err)
	assert.Equal(t, testContent, data)
}

func TestFilesystemUploader_Upload_NonExistentFile(t *testing.T) {
	// given
	destDir := t.TempDir()

	uploader, err := NewFilesystemUploader(destDir)
	assert.NoError(t, err)

	// when
	err = uploader.Upload(context.Background(), "/nonexistent/file/path.txt")

	// then
	assert.Error(t, err)
}
