package files

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
)

// Uploader handles uploading files to a destination.
type Uploader interface {
	Upload(ctx context.Context, localPath, remoteKey string) error
}

// blobUploader implements Uploader for cloud blob storage.
type blobUploader struct {
	bucket *blob.Bucket
}

// NewBlobUploader creates a new Uploader that uploads files to a blob bucket.
func NewBlobUploader(bucket *blob.Bucket) Uploader {
	return &blobUploader{bucket: bucket}
}

func (u *blobUploader) Upload(ctx context.Context, localPath, remoteKey string) error {
	filename := filepath.Base(localPath)

	data, err := os.ReadFile(localPath) //nolint:gosec // localPath is intentionally provided by caller
	if err != nil {
		logrus.WithError(err).WithField("file", filename).Error("failed to read file for upload")
		return fmt.Errorf("reading file for upload: %w", err)
	}

	if err := u.bucket.WriteAll(ctx, remoteKey, data, nil); err != nil {
		return fmt.Errorf("uploading file to blob storage: %w", err)
	}

	if err := os.Remove(localPath); err != nil {
		logrus.WithError(err).WithField("file", filename).Warn("uploaded but failed to delete local file")
		return fmt.Errorf("deleting local file after upload: %w", err)
	}

	return nil
}

// filesystemUploader implements Uploader for local filesystem storage.
type filesystemUploader struct {
	destDir  string
	renameFn func(string, string) error
}

// NewFilesystemUploader creates a new Uploader that moves files to a destination directory.
func NewFilesystemUploader(destDir string) (Uploader, error) {
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return nil, fmt.Errorf("creating destination directory: %w", err)
	}
	return &filesystemUploader{destDir: destDir, renameFn: os.Rename}, nil
}

func (u *filesystemUploader) Upload(ctx context.Context, localPath, remoteKey string) error {
	filename := filepath.Base(remoteKey)
	destPath := filepath.Join(u.destDir, filename)
	if err := u.renameFn(localPath, destPath); err != nil {
		var linkErr *os.LinkError
		if errors.As(err, &linkErr) && errors.Is(linkErr.Err, syscall.EXDEV) {
			// Cross-device move: fall back to copy-then-delete
			if copyErr := copyAndDelete(localPath, destPath); copyErr != nil {
				return fmt.Errorf("copy fallback after EXDEV: %w", copyErr)
			}
			logrus.Infof("moved file to filesystem destination (copy fallback)")
			return nil
		}
		return fmt.Errorf("moving file to filesystem destination: %w", err)
	}
	logrus.Infof("moved file to filesystem destination")
	return nil
}

// copyAndDelete copies src to dst and removes src on success.
func copyAndDelete(src, dst string) error {
	srcFile, err := os.Open(src) //nolint:gosec // path is controlled
	if err != nil {
		return fmt.Errorf("opening source: %w", err)
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.Create(dst) //nolint:gosec // path is controlled
	if err != nil {
		return fmt.Errorf("creating destination: %w", err)
	}

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		_ = dstFile.Close()
		_ = os.Remove(dst)
		return fmt.Errorf("copying data: %w", err)
	}

	if err := dstFile.Close(); err != nil {
		_ = os.Remove(dst)
		return fmt.Errorf("closing destination: %w", err)
	}

	if err := os.Remove(src); err != nil {
		return fmt.Errorf("removing source after copy: %w", err)
	}

	return nil
}
