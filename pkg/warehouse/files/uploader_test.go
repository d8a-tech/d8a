package files

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

const testRemoteKey = "table=events/schema=abc123/dt=2026/02/25/seg-id.csv"

func TestBlobStreamUploader_BeginCommit_Success(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	uploader := NewBlobUploader(bucket)
	upload, err := uploader.Begin(context.Background(), testRemoteKey)
	require.NoError(t, err)

	_, err = upload.Writer().Write([]byte("test,data\n1,2\n"))
	require.NoError(t, err)
	require.NoError(t, upload.Commit())

	stored, err := bucket.ReadAll(context.Background(), testRemoteKey)
	require.NoError(t, err)
	assert.Equal(t, []byte("test,data\n1,2\n"), stored)

	require.Error(t, upload.Abort())
}

func TestBlobStreamUploader_Abort_Success(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	uploader := NewBlobUploader(bucket)
	upload, err := uploader.Begin(context.Background(), testRemoteKey)
	require.NoError(t, err)

	_, err = upload.Writer().Write([]byte("test,data\n1,2\n"))
	require.NoError(t, err)
	require.NoError(t, upload.Abort())
	require.NoError(t, upload.Abort())

	_, err = bucket.ReadAll(context.Background(), testRemoteKey)
	require.Error(t, err)

	require.Error(t, upload.Commit())
}

func TestBlobStreamUploader_Begin_Error(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	require.NoError(t, bucket.Close())

	uploader := &blobStreamUploader{bucket: bucket}
	_, err := uploader.Begin(context.Background(), testRemoteKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating blob writer")
}

func TestBlobStreamUploader_CommitFailure_AllowsAbort(t *testing.T) {
	upload := &blobUpload{
		writer: failingWriteCloser{err: errors.New("close failed")},
		cancel: func() {},
	}

	err := upload.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closing blob writer")

	err = upload.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closing blob writer")

	err = upload.Abort()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closing blob writer")

	err = upload.Abort()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closing blob writer")
}

func TestFilesystemStreamUploader_Commit_SuccessNestedDirs(t *testing.T) {
	destDir := t.TempDir()
	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	upload, err := uploader.Begin(context.Background(), testRemoteKey)
	require.NoError(t, err)

	_, err = upload.Writer().Write([]byte("test data"))
	require.NoError(t, err)
	require.NoError(t, upload.Commit())

	destPath := filepath.Join(destDir, filepath.FromSlash(testRemoteKey))
	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("test data"), data)

	require.Error(t, upload.Abort())
}

func TestFilesystemStreamUploader_Abort_CleansUpTempFile(t *testing.T) {
	destDir := t.TempDir()
	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	upload, err := uploader.Begin(context.Background(), testRemoteKey)
	require.NoError(t, err)

	fu, ok := upload.(*filesystemUpload)
	require.True(t, ok)
	tempPath := fu.tempPath

	_, err = upload.Writer().Write([]byte("test data"))
	require.NoError(t, err)
	require.NoError(t, upload.Abort())
	require.NoError(t, upload.Abort())

	_, err = os.Stat(tempPath)
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))

	require.Error(t, upload.Commit())
}

func TestFilesystemStreamUploader_Begin_RejectsPathTraversal(t *testing.T) {
	destDir := t.TempDir()
	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	_, err = uploader.Begin(context.Background(), "../segment.csv")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid filesystem destination key")
}

func TestFilesystemStreamUploader_Commit_CrossDeviceCopyFallback(t *testing.T) {
	destDir := t.TempDir()
	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	fsUploader, ok := uploader.(*filesystemStreamUploader)
	require.True(t, ok)
	fsUploader.renameFn = func(src, dst string) error {
		return &os.LinkError{Op: "rename", Old: src, New: dst, Err: syscall.EXDEV}
	}

	upload, err := uploader.Begin(context.Background(), testRemoteKey)
	require.NoError(t, err)

	fu, ok := upload.(*filesystemUpload)
	require.True(t, ok)
	tempPath := fu.tempPath

	_, err = upload.Writer().Write([]byte("cross-device test data"))
	require.NoError(t, err)
	require.NoError(t, upload.Commit())

	destPath := filepath.Join(destDir, filepath.FromSlash(testRemoteKey))
	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("cross-device test data"), data)

	_, err = os.Stat(tempPath)
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestFilesystemUpload_Abort_AlreadyClosedFile(t *testing.T) {
	destDir := t.TempDir()
	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	upload, err := uploader.Begin(context.Background(), testRemoteKey)
	require.NoError(t, err)

	fu, ok := upload.(*filesystemUpload)
	require.True(t, ok)
	require.NoError(t, fu.file.Close())
	require.NoError(t, upload.Abort())
}

func TestFilesystemStreamUploader_CommitFailure_AllowsAbortAndCleanup(t *testing.T) {
	destDir := t.TempDir()
	uploader, err := NewFilesystemUploader(destDir)
	require.NoError(t, err)

	fsUploader, ok := uploader.(*filesystemStreamUploader)
	require.True(t, ok)
	fsUploader.renameFn = func(src, dst string) error {
		return &os.LinkError{Op: "rename", Old: src, New: dst, Err: syscall.EIO}
	}

	upload, err := uploader.Begin(context.Background(), testRemoteKey)
	require.NoError(t, err)

	fu, ok := upload.(*filesystemUpload)
	require.True(t, ok)
	tempPath := fu.tempPath

	_, err = upload.Writer().Write([]byte("test data"))
	require.NoError(t, err)

	err = upload.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "moving file to filesystem destination")

	err = upload.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "moving file to filesystem destination")

	require.NoError(t, upload.Abort())

	_, err = os.Stat(tempPath)
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))

	err = upload.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload already aborted")
}

type failingWriteCloser struct {
	err error
}

func (w failingWriteCloser) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (w failingWriteCloser) Close() error {
	return w.err
}

var _ io.WriteCloser = failingWriteCloser{}
