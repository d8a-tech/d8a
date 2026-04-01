package files

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"gocloud.dev/blob"
)

// StreamUploader handles uploading streams to a destination.
type StreamUploader interface {
	Begin(ctx context.Context, key string) (Upload, error)
}

// Upload represents a single upload transaction.
type Upload interface {
	Writer() io.Writer
	Commit() error
	Abort() error
}

// blobStreamUploader implements StreamUploader for cloud blob storage.
type blobStreamUploader struct {
	bucket *blob.Bucket
}

// NewBlobUploader creates a new StreamUploader that uploads files to a blob bucket.
func NewBlobUploader(bucket *blob.Bucket) StreamUploader {
	return &blobStreamUploader{bucket: bucket}
}

func (u *blobStreamUploader) Begin(ctx context.Context, key string) (Upload, error) {
	childCtx, cancel := context.WithCancel(ctx)
	bw, err := u.bucket.NewWriter(childCtx, key, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating blob writer: %w", err)
	}

	return &blobUpload{
		writer: bw,
		cancel: cancel,
	}, nil
}

type blobUpload struct {
	writer io.WriteCloser
	cancel context.CancelFunc

	mu        sync.Mutex
	state     uploadState
	commitErr error
	abortErr  error
}

func (u *blobUpload) Writer() io.Writer {
	return u.writer
}

func (u *blobUpload) Commit() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.state == uploadStateAborted {
		return errors.New("upload already aborted")
	}
	if u.state == uploadStateCommitted {
		return nil
	}
	if u.commitErr != nil {
		return u.commitErr
	}

	if err := u.writer.Close(); err != nil {
		u.commitErr = fmt.Errorf("closing blob writer: %w", err)
		return u.commitErr
	}
	u.cancel()
	u.state = uploadStateCommitted
	return nil
}

func (u *blobUpload) Abort() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.state == uploadStateCommitted {
		return errors.New("upload already committed")
	}
	if u.state == uploadStateAborted {
		return u.abortErr
	}
	u.commitErr = nil

	u.cancel()
	closeErr := u.writer.Close()
	u.state = uploadStateAborted

	if closeErr == nil || errors.Is(closeErr, context.Canceled) {
		u.abortErr = nil
		return nil
	}

	u.abortErr = fmt.Errorf("closing blob writer: %w", closeErr)
	return u.abortErr
}

// filesystemStreamUploader implements StreamUploader for local filesystem storage.
type filesystemStreamUploader struct {
	destDir  string
	renameFn func(string, string) error
}

// NewFilesystemUploader creates a new StreamUploader that writes to a destination directory.
func NewFilesystemUploader(destDir string) (StreamUploader, error) {
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return nil, fmt.Errorf("creating destination directory: %w", err)
	}
	return &filesystemStreamUploader{destDir: destDir, renameFn: os.Rename}, nil
}

func (u *filesystemStreamUploader) Begin(ctx context.Context, remoteKey string) (Upload, error) {
	_ = ctx

	relPath := filepath.Clean(filepath.FromSlash(remoteKey))
	if relPath == "." || filepath.IsAbs(relPath) || relPath == ".." ||
		strings.HasPrefix(relPath, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("invalid filesystem destination key: %s", remoteKey)
	}

	destPath := filepath.Join(u.destDir, relPath)
	tmpFile, err := os.CreateTemp(u.destDir, "upload-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("creating temp upload file: %w", err)
	}

	return &filesystemUpload{
		tempPath: tmpFile.Name(),
		file:     tmpFile,
		destPath: destPath,
		renameFn: u.renameFn,
	}, nil
}

type uploadState uint8

const (
	uploadStateOpen uploadState = iota
	uploadStateCommitted
	uploadStateAborted
)

type filesystemUpload struct {
	tempPath string
	destPath string
	file     *os.File
	renameFn func(string, string) error

	mu        sync.Mutex
	state     uploadState
	commitErr error
	abortErr  error
}

func (u *filesystemUpload) Writer() io.Writer {
	return u.file
}

func (u *filesystemUpload) Commit() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.state == uploadStateAborted {
		return errors.New("upload already aborted")
	}
	if u.state == uploadStateCommitted {
		return nil
	}
	if u.commitErr != nil {
		return u.commitErr
	}

	if err := u.file.Close(); err != nil {
		u.commitErr = fmt.Errorf("closing temp upload file: %w", err)
		return u.commitErr
	}

	if err := os.MkdirAll(filepath.Dir(u.destPath), 0o750); err != nil {
		u.commitErr = fmt.Errorf("creating destination directory: %w", err)
		return u.commitErr
	}

	if err := u.renameFn(u.tempPath, u.destPath); err != nil {
		var linkErr *os.LinkError
		if errors.As(err, &linkErr) && errors.Is(linkErr.Err, syscall.EXDEV) {
			if copyErr := copyAndDelete(u.tempPath, u.destPath); copyErr != nil {
				u.commitErr = fmt.Errorf("copy fallback after EXDEV: %w", copyErr)
				return u.commitErr
			}
			u.state = uploadStateCommitted
			return nil
		}

		u.commitErr = fmt.Errorf("moving file to filesystem destination: %w", err)
		return u.commitErr
	}

	u.state = uploadStateCommitted
	return nil
}

func (u *filesystemUpload) Abort() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.state == uploadStateCommitted {
		return errors.New("upload already committed")
	}
	if u.state == uploadStateAborted {
		return u.abortErr
	}
	u.commitErr = nil

	closeErr := u.file.Close()
	removeErr := os.Remove(u.tempPath)
	u.state = uploadStateAborted

	if closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
		u.abortErr = fmt.Errorf("closing temp upload file: %w", closeErr)
		return u.abortErr
	}

	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		u.abortErr = fmt.Errorf("removing temp upload file: %w", removeErr)
		return u.abortErr
	}

	u.abortErr = nil
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
