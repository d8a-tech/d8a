package files

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

const testCSVFilename = "test.csv"
const testRemoteKey = "table=events/schema=abc123/dt=2026/02/25/seg-id.csv"

func TestBlobUploader_Upload_Success(t *testing.T) {
	// given
	tempDir := t.TempDir()
	testContent := []byte("test,data\n1,2\n")
	filePath := filepath.Join(tempDir, testCSVFilename)
	err := os.WriteFile(filePath, testContent, 0o644)
	assert.NoError(t, err)

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	uploader := NewBlobUploader(bucket)

	// when
	err = uploader.Upload(context.Background(), filePath, testRemoteKey)

	// then
	assert.NoError(t, err)

	// Verify data was written to bucket
	storedData, err := bucket.ReadAll(context.Background(), testRemoteKey)
	assert.NoError(t, err)
	assert.Equal(t, testContent, storedData)

	// Verify local file was deleted
	_, err = os.Stat(filePath)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestBlobUploader_Upload_UploadError(t *testing.T) {
	// given
	tempDir := t.TempDir()
	testContent := []byte("test,data\n1,2\n")
	filePath := filepath.Join(tempDir, testCSVFilename)
	err := os.WriteFile(filePath, testContent, 0o644)
	assert.NoError(t, err)

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	// Close bucket to trigger write error
	err = bucket.Close()
	assert.NoError(t, err)

	uploader := NewBlobUploader(bucket)

	// when
	err = uploader.Upload(context.Background(), filePath, testRemoteKey)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "uploading file to blob storage")

	// Verify local file was NOT deleted
	_, err = os.Stat(filePath)
	assert.NoError(t, err)
}

func TestBlobUploader_Upload_DeleteErrorAfterUpload(t *testing.T) {
	// given
	tempDir := t.TempDir()
	testContent := []byte("test,data\n1,2\n")
	filePath := filepath.Join(tempDir, testCSVFilename)
	err := os.WriteFile(filePath, testContent, 0o644)
	assert.NoError(t, err)

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	// Make file read-only in its directory by removing write permissions
	err = os.Chmod(filePath, 0o444)
	assert.NoError(t, err)
	// Remove write permission from directory to prevent deletion
	err = os.Chmod(tempDir, 0o555)
	assert.NoError(t, err)
	// Restore permissions in cleanup
	t.Cleanup(func() {
		_ = os.Chmod(tempDir, 0o755)
		_ = os.Chmod(filePath, 0o644)
	})

	uploader := NewBlobUploader(bucket)

	// when
	err = uploader.Upload(context.Background(), filePath, testRemoteKey)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deleting local file")

	// Verify data was written to bucket
	storedData, err := bucket.ReadAll(context.Background(), testRemoteKey)
	assert.NoError(t, err)
	assert.Equal(t, testContent, storedData)

	// File should still exist (couldn't be deleted)
	_, statErr := os.Stat(filePath)
	assert.NoError(t, statErr)
}

func TestBlobUploader_Upload_NonExistentFile(t *testing.T) {
	// given
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	uploader := NewBlobUploader(bucket)

	// when
	err := uploader.Upload(context.Background(), "/nonexistent/file/path.csv", testRemoteKey)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reading file for upload")
}

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
	err = uploader.Upload(context.Background(), srcFilePath, testRemoteKey)

	// then
	assert.NoError(t, err)
	// Verify the file was moved (no longer exists at source)
	_, err = os.Stat(srcFilePath)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
	// Verify the file exists at destination
	destFilePath := filepath.Join(destDir, filepath.FromSlash(testRemoteKey))
	data, err := os.ReadFile(destFilePath)
	assert.NoError(t, err)
	assert.Equal(t, testContent, data)
}

func TestFilesystemUploader_Upload_CreatesNestedDestinationDirs(t *testing.T) {
	// given
	srcDir := t.TempDir()
	destDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "segment.csv")
	testContent := []byte("test data")
	require.NoError(t, os.WriteFile(srcPath, testContent, 0o644))

	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	remoteKey := "table=events/schema=abc123/y=2026/m=03/d=13/segment.csv"

	// when
	err = uploader.Upload(context.Background(), srcPath, remoteKey)

	// then
	require.NoError(t, err)
	destPath := filepath.Join(destDir, filepath.FromSlash(remoteKey))
	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, testContent, data)
}

func TestFilesystemUploader_Upload_RejectsPathTraversalRemoteKey(t *testing.T) {
	// given
	srcDir := t.TempDir()
	destDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "segment.csv")
	require.NoError(t, os.WriteFile(srcPath, []byte("test data"), 0o644))

	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	// when
	err = uploader.Upload(context.Background(), srcPath, "../segment.csv")

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid filesystem destination key")
	assert.FileExists(t, srcPath)
}

func TestFilesystemUploader_Upload_NonExistentFile(t *testing.T) {
	// given
	destDir := t.TempDir()

	uploader, err := NewFilesystemUploader(destDir)
	assert.NoError(t, err)

	// when
	err = uploader.Upload(context.Background(), "/nonexistent/file/path.txt", testRemoteKey)

	// then
	assert.Error(t, err)
}

func TestFilesystemUploader_Upload_CrossDevice_CopyFallback(t *testing.T) {
	// given
	srcDir := t.TempDir()
	destDir := t.TempDir()

	testContent := []byte("cross-device test data")
	srcPath := filepath.Join(srcDir, "test.csv")
	require.NoError(t, os.WriteFile(srcPath, testContent, 0o644))

	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	// Override renameFn to simulate EXDEV
	fsUp, ok := uploader.(*filesystemUploader)
	require.True(t, ok)
	fsUp.renameFn = func(src, dst string) error {
		return &os.LinkError{Op: "rename", Old: src, New: dst, Err: syscall.EXDEV}
	}

	// when
	err = uploader.Upload(context.Background(), srcPath, testRemoteKey)

	// then
	require.NoError(t, err)

	// Destination file should exist with correct content
	destPath := filepath.Join(destDir, filepath.FromSlash(testRemoteKey))
	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, testContent, data)

	// Source file should be deleted
	_, err = os.Stat(srcPath)
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}
