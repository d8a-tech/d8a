package files

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

// EnsureStreamDirs creates the directory structure for a stream.
func EnsureStreamDirs(spoolDir, tableEsc, fingerprint string) error {
	streamDir := StreamDir(spoolDir, tableEsc, fingerprint)
	paths := []string{
		streamDir,
		SealedDir(spoolDir, tableEsc, fingerprint),
		UploadingDir(spoolDir, tableEsc, fingerprint),
		FailedDir(spoolDir, tableEsc, fingerprint),
	}

	for _, path := range paths {
		if err := os.MkdirAll(path, 0o750); err != nil {
			return fmt.Errorf("creating stream dir %s: %w", path, err)
		}
	}

	return nil
}

// SaveMetadataFile writes metadata to a file atomically using a temporary file.
// Atomic write (tmp + rename) prevents readers from seeing incomplete metadata.
func SaveMetadataFile(path string, metadata *Metadata) error {
	tmpPath := path + ".tmp"

	// Write to temporary file
	tmpFile, err := os.Create(tmpPath) //nolint:gosec // tmpPath is controlled, not user input
	if err != nil {
		return fmt.Errorf("creating metadata temp file: %w", err)
	}

	if err := WriteMetadata(tmpFile, metadata); err != nil {
		_ = tmpFile.Close()    // Best effort close, error on write is more important
		_ = os.Remove(tmpPath) // Best effort cleanup
		return fmt.Errorf("writing metadata content: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath) // Best effort cleanup
		return fmt.Errorf("closing metadata temp file: %w", err)
	}

	// Atomically rename to final location
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath) // Best effort cleanup
		return fmt.Errorf("moving metadata file to final location: %w", err)
	}

	logrus.WithField("path", path).Debug("saved metadata file")

	return nil
}

// LoadMetadataFile reads metadata from a file.
func LoadMetadataFile(metaPath string) (*Metadata, error) {
	file, err := os.Open(metaPath) //nolint:gosec // metaPath is controlled, not user input
	if err != nil {
		return nil, fmt.Errorf("opening metadata file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			logrus.WithError(closeErr).Error("closing metadata file")
		}
	}()

	metadata, err := ReadMetadata(file)
	if err != nil {
		return nil, fmt.Errorf("reading metadata file: %w", err)
	}

	return metadata, nil
}

// DeleteMetadataFile removes a metadata file from disk.
func DeleteMetadataFile(metaPath string) error {
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting metadata file: %w", err)
	}
	logrus.WithField("path", metaPath).Debug("deleted metadata file")
	return nil
}

func findSealedSegments(sealedDir string) ([]string, error) {
	entries, err := os.ReadDir(sealedDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("reading sealed dir %s: %w", sealedDir, err)
	}

	segments := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != ".csv" {
			continue
		}
		segments = append(segments, strings.TrimSuffix(name, ".csv"))
	}

	return segments, nil
}
