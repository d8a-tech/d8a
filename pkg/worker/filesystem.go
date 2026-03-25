package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// FilesystemDirectoryPublisher writes tasks to timestamped files in a directory.
type FilesystemDirectoryPublisher struct {
	dir    string
	format MessageFormat
}

// NewFilesystemDirectoryPublisher creates a publisher that writes tasks to timestamped files.
func NewFilesystemDirectoryPublisher(dir string, format MessageFormat) (*FilesystemDirectoryPublisher, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}
	return &FilesystemDirectoryPublisher{dir: dir, format: format}, nil
}

// Publish implements Publisher.
func (p *FilesystemDirectoryPublisher) Publish(task *Task) error {
	data, err := p.format.Serialize(task)
	if err != nil {
		return fmt.Errorf("failed to serialize task: %w", err)
	}
	timestamp := time.Now().UnixNano()
	tmpFilename := fmt.Sprintf("%d.tmp", timestamp)
	taskFilename := fmt.Sprintf("%d.task", timestamp)
	tmpPath := filepath.Join(p.dir, tmpFilename)
	taskPath := filepath.Join(p.dir, taskFilename)

	//nolint:gosec // path is constructed from controlled directory
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("failed to open temp task file: %w", err)
	}

	cleanupTmpFile := func() {
		if removeErr := os.Remove(tmpPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			logrus.Warnf("failed to remove temp task file %s: %v", tmpFilename, removeErr)
		}
	}

	if _, err := file.Write(data); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			logrus.Warnf("failed to close temp task file %s: %v", tmpFilename, closeErr)
		}
		cleanupTmpFile()
		return fmt.Errorf("failed to write temp task file: %w", err)
	}

	if err := file.Sync(); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			logrus.Warnf("failed to close temp task file %s: %v", tmpFilename, closeErr)
		}
		cleanupTmpFile()
		return fmt.Errorf("failed to sync temp task file: %w", err)
	}

	if err := file.Close(); err != nil {
		cleanupTmpFile()
		return fmt.Errorf("failed to close temp task file: %w", err)
	}

	if err := os.Rename(tmpPath, taskPath); err != nil {
		cleanupTmpFile()
		return fmt.Errorf("failed to rename temp task file: %w", err)
	}
	return nil
}

// FilesystemDirectoryConsumer reads tasks from files in a directory, ordered by timestamp.
type FilesystemDirectoryConsumer struct {
	ctx       context.Context
	dir       string
	format    MessageFormat
	pollDelay time.Duration
}

// NewFilesystemDirectoryConsumer creates a consumer that reads tasks from timestamped files.
func NewFilesystemDirectoryConsumer(
	ctx context.Context,
	dir string,
	format MessageFormat,
) (*FilesystemDirectoryConsumer, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory does not exist: %s", dir)
	}
	return &FilesystemDirectoryConsumer{
		ctx:       ctx,
		dir:       dir,
		format:    format,
		pollDelay: 10 * time.Millisecond,
	}, nil
}

// Consume implements Consumer.
func (c *FilesystemDirectoryConsumer) Consume(handler TaskHandlerFunc) error {
	for {
		select {
		case <-c.ctx.Done():
			logrus.Debugf("filesystemDirectoryConsumer stopping due to context done")
			return nil
		default:
			processed, err := c.processNextBatch(handler)
			if err != nil {
				logrus.Errorf("filesystemDirectoryConsumer error: %v", err)
				return err
			}
			if !processed {
				time.Sleep(c.pollDelay)
			}
		}
	}
}

func (c *FilesystemDirectoryConsumer) processNextBatch(handler TaskHandlerFunc) (bool, error) {
	files, err := c.listTaskFiles()
	if err != nil {
		return false, err
	}
	if len(files) == 0 {
		return false, nil
	}
	for _, file := range files {
		path := filepath.Join(c.dir, file)
		data, err := os.ReadFile(path) //nolint:gosec // path is constructed from controlled directory
		if err != nil {
			return false, fmt.Errorf("failed to read task file %s: %w", file, err)
		}
		if len(data) == 0 {
			logrus.Warnf("discarding empty task file %s (possibly from incomplete write)", file)
			if err := os.Remove(path); err != nil {
				return false, fmt.Errorf("failed to remove empty file %s: %w", file, err)
			}
			continue
		}
		task, err := c.format.Deserialize(data)
		if err != nil {
			return false, fmt.Errorf("failed to deserialize task from %s: %w", file, err)
		}
		if err := handler(task); err != nil {
			return false, fmt.Errorf("handler error for %s: %w", file, err)
		}
		if err := os.Remove(path); err != nil {
			return false, fmt.Errorf("failed to remove processed file %s: %w", file, err)
		}
	}
	return true, nil
}

func (c *FilesystemDirectoryConsumer) listTaskFiles() ([]string, error) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".task") {
			files = append(files, entry.Name())
		}
	}
	sort.Slice(files, func(i, j int) bool {
		tsI := extractTimestamp(files[i])
		tsJ := extractTimestamp(files[j])
		return tsI < tsJ
	})
	return files, nil
}

func extractTimestamp(filename string) int64 {
	name := strings.TrimSuffix(filename, ".task")
	ts, _ := strconv.ParseInt(name, 10, 64)
	return ts
}
