package files

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
)

// Uploader handles uploading files to a destination.
// It receives file paths, not rows, and is responsible for moving or
// copying files from a temporary location to a final destination.
type Uploader interface {
	// Upload moves or uploads a file to its final destination.
	// filePath is the path to the file on the local filesystem.
	Upload(ctx context.Context, filePath string) error
}

// blobUploader implements Uploader for cloud blob storage.
type blobUploader struct {
	bucket *blob.Bucket
}

// NewBlobUploader creates a new Uploader that uploads files to a blob bucket.
func NewBlobUploader(bucket *blob.Bucket) Uploader {
	return &blobUploader{bucket: bucket}
}

// Upload implements Uploader.
func (u *blobUploader) Upload(ctx context.Context, filePath string) error {
	logrus.Infof("blob uploader stub: would upload file to bucket")
	return nil
}

// filesystemUploader implements Uploader for local filesystem storage.
type filesystemUploader struct {
	destDir string
}

// NewFilesystemUploader creates a new Uploader that moves files to a destination directory.
// It creates the destination directory if it does not exist.
func NewFilesystemUploader(destDir string) (Uploader, error) {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return nil, fmt.Errorf("creating destination directory: %w", err)
	}
	return &filesystemUploader{destDir: destDir}, nil
}

// Upload implements Uploader.
func (u *filesystemUploader) Upload(ctx context.Context, filePath string) error {
	filename := filepath.Base(filePath)
	destPath := filepath.Join(u.destDir, filename)
	if err := os.Rename(filePath, destPath); err != nil {
		return fmt.Errorf("moving file to filesystem destination: %w", err)
	}
	logrus.Infof("moved file to filesystem destination")
	return nil
}
