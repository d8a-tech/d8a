package files

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

// SaveMetadataFile writes metadata to a file atomically using a temporary file.
// Atomic write (tmp + rename) prevents flush timer from reading incomplete metadata.
func SaveMetadataFile(spoolDir, table, fingerprint, schemaB64 string) (string, error) {
	metadata := &Metadata{
		Table:       table,
		Fingerprint: fingerprint,
		Schema:      schemaB64,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	metaFilename := MetadataFilename(table, fingerprint)
	metaPath := filepath.Join(spoolDir, metaFilename)
	tmpPath := metaPath + ".tmp"

	// Write to temporary file
	tmpFile, err := os.Create(tmpPath) //nolint:gosec // tmpPath is controlled, not user input
	if err != nil {
		return "", fmt.Errorf("creating metadata temp file: %w", err)
	}

	if err := WriteMetadata(tmpFile, metadata); err != nil {
		_ = tmpFile.Close()    // Best effort close, error on write is more important
		_ = os.Remove(tmpPath) // Best effort cleanup
		return "", fmt.Errorf("writing metadata content: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath) // Best effort cleanup
		return "", fmt.Errorf("closing metadata temp file: %w", err)
	}

	// Atomically rename to final location
	if err := os.Rename(tmpPath, metaPath); err != nil {
		_ = os.Remove(tmpPath) // Best effort cleanup
		return "", fmt.Errorf("moving metadata file to final location: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"table":       table,
		"fingerprint": fingerprint,
		"path":        metaPath,
	}).Debug("saved metadata file")

	return metaPath, nil
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

// FindCSVFiles finds all CSV files in the spool directory.
func FindCSVFiles(spoolDir string) ([]string, error) {
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		return nil, fmt.Errorf("reading spool directory: %w", err)
	}

	var csvFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == ".csv" {
			csvFiles = append(csvFiles, filepath.Join(spoolDir, entry.Name()))
		}
	}

	return csvFiles, nil
}

// GetMetadataPathForCSV returns the metadata file path for a given CSV file.
func GetMetadataPathForCSV(csvPath string) string {
	dir := filepath.Dir(csvPath)
	base := filepath.Base(csvPath)
	metaName := base[:len(base)-4] + ".meta.json"
	return filepath.Join(dir, metaName)
}
