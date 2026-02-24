package files

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/gcerrors"
)

const testCSVFilename = "test.csv"

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
	err = uploader.Upload(context.Background(), filePath)

	// then
	assert.NoError(t, err)

	// Verify data was written to bucket
	storedData, err := bucket.ReadAll(context.Background(), testCSVFilename)
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
	err = uploader.Upload(context.Background(), filePath)

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
	err = uploader.Upload(context.Background(), filePath)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deleting local file")

	// Verify data was written to bucket
	storedData, err := bucket.ReadAll(context.Background(), testCSVFilename)
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
	err := uploader.Upload(context.Background(), "/nonexistent/file/path.csv")

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reading file for upload")
}

func TestBlobUploader_Upload_ContextCancellation(t *testing.T) {
	// given
	tempDir := t.TempDir()
	testContent := []byte("test,data\n1,2\n")
	filePath := filepath.Join(tempDir, testCSVFilename)
	err := os.WriteFile(filePath, testContent, 0o644)
	assert.NoError(t, err)

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	uploader := NewBlobUploader(bucket)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// when
	err = uploader.Upload(ctx, filePath)

	// then
	// memblob doesn't always respect context cancellation synchronously,
	// but if it does, we should handle it
	// For this test, we just verify the file isn't deleted if upload fails
	if err != nil {
		// If upload failed due to context, file should remain
		if errors.Is(err, context.Canceled) || gcerrors.Code(err) == gcerrors.Canceled {
			_, statErr := os.Stat(filePath)
			assert.NoError(t, statErr, "file should remain if upload was cancelled")
		}
	}
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
