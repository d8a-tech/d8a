package files

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testTableName = "events"

// TestSaveMetadataFile_CreatesFile verifies metadata file is created successfully.
func TestSaveMetadataFile_CreatesFile(t *testing.T) {
	// given: a temporary spool directory
	spoolDir := t.TempDir()

	// when: saving metadata file
	metaPath, err := SaveMetadataFile(spoolDir, testTableName, "a3b5c7f9e1d4b2a6", "base64-schema")

	// then: file is created successfully
	assert.NoError(t, err)
	assert.NotEmpty(t, metaPath)
	assert.True(t, filepath.IsAbs(metaPath))

	// File should exist at the expected path
	expectedPath := filepath.Join(spoolDir, "a3b5c7f9e1d4b2a6_events.meta.json")
	assert.Equal(t, expectedPath, metaPath)

	// Verify file actually exists
	stat, err := os.Stat(metaPath)
	assert.NoError(t, err)
	assert.False(t, stat.IsDir())
	assert.Greater(t, stat.Size(), int64(0))
}

// TestSaveMetadataFile_AtomicWrite verifies file is written atomically.
func TestSaveMetadataFile_AtomicWrite(t *testing.T) {
	// given: a temporary spool directory
	spoolDir := t.TempDir()

	// when: saving metadata file
	metaPath, err := SaveMetadataFile(spoolDir, testTableName, "a3b5c7f9e1d4b2a6", "base64-schema")
	assert.NoError(t, err)

	// then: only final file exists, not temp file
	tempPath := metaPath + ".tmp"
	_, err = os.Stat(tempPath)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))

	// But final file exists
	_, err = os.Stat(metaPath)
	assert.NoError(t, err)
}

// TestSaveMetadataFile_ContainsCorrectData verifies metadata content is correct.
func TestSaveMetadataFile_ContainsCorrectData(t *testing.T) {
	// given: a temporary spool directory
	spoolDir := t.TempDir()
	expectedFingerprint := "a3b5c7f9e1d4b2a6"
	expectedSchema := "base64-encoded-schema"

	// when: saving metadata file
	metaPath, err := SaveMetadataFile(spoolDir, testTableName, expectedFingerprint, expectedSchema)
	assert.NoError(t, err)

	// then: file contains correct data
	metadata, err := LoadMetadataFile(metaPath)
	assert.NoError(t, err)
	assert.Equal(t, testTableName, metadata.Table)
	assert.Equal(t, expectedFingerprint, metadata.Fingerprint)
	assert.Equal(t, expectedSchema, metadata.Schema)
	assert.NotEmpty(t, metadata.CreatedAt) // Should have timestamp
}

// TestSaveMetadataFile_InvalidDirectory returns error for nonexistent directory.
func TestSaveMetadataFile_InvalidDirectory(t *testing.T) {
	// given: a nonexistent directory
	invalidDir := "/nonexistent/directory/path"

	// when: saving metadata file
	_, err := SaveMetadataFile(invalidDir, testTableName, "fingerprint", "schema")

	// then: error is returned
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "creating metadata temp file")
}

// TestLoadMetadataFile_Success verifies metadata file is loaded correctly.
func TestLoadMetadataFile_Success(t *testing.T) {
	// given: a saved metadata file
	spoolDir := t.TempDir()
	expectedFingerprint := "a3b5c7f9e1d4b2a6"
	expectedSchema := "base64-schema"

	metaPath, err := SaveMetadataFile(spoolDir, testTableName, expectedFingerprint, expectedSchema)
	assert.NoError(t, err)

	// when: loading the metadata file
	metadata, err := LoadMetadataFile(metaPath)

	// then: metadata is loaded and matches saved data
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, testTableName, metadata.Table)
	assert.Equal(t, expectedFingerprint, metadata.Fingerprint)
	assert.Equal(t, expectedSchema, metadata.Schema)
}

// TestLoadMetadataFile_NonexistentFile returns error for missing file.
func TestLoadMetadataFile_NonexistentFile(t *testing.T) {
	// given: a nonexistent file path
	nonexistentPath := "/nonexistent/metadata.json"

	// when: loading the metadata file
	_, err := LoadMetadataFile(nonexistentPath)

	// then: error is returned
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "opening metadata file")
}

// TestLoadMetadataFile_MalformedFile returns error for invalid JSON.
func TestLoadMetadataFile_MalformedFile(t *testing.T) {
	// given: a file with invalid JSON
	spoolDir := t.TempDir()
	metaPath := filepath.Join(spoolDir, "malformed.meta.json")
	err := os.WriteFile(metaPath, []byte("not valid json"), 0o644)
	assert.NoError(t, err)

	// when: loading the metadata file
	_, err = LoadMetadataFile(metaPath)

	// then: error is returned for invalid JSON
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reading metadata file")
}

// TestDeleteMetadataFile_Success verifies metadata file is deleted.
func TestDeleteMetadataFile_Success(t *testing.T) {
	// given: a saved metadata file
	spoolDir := t.TempDir()
	metaPath, err := SaveMetadataFile(spoolDir, testTableName, "fingerprint", "schema")
	assert.NoError(t, err)

	// Verify file exists before deletion
	_, err = os.Stat(metaPath)
	assert.NoError(t, err)

	// when: deleting the metadata file
	err = DeleteMetadataFile(metaPath)

	// then: file is deleted successfully
	assert.NoError(t, err)
	_, err = os.Stat(metaPath)
	assert.True(t, os.IsNotExist(err))
}

// TestDeleteMetadataFile_NonexistentFile_NoError verifies no error for nonexistent file.
func TestDeleteMetadataFile_NonexistentFile_NoError(t *testing.T) {
	// given: a nonexistent file path
	nonexistentPath := "/nonexistent/metadata.json"

	// when: deleting the nonexistent file
	err := DeleteMetadataFile(nonexistentPath)

	// then: no error is returned (idempotent behavior)
	assert.NoError(t, err)
}

// TestFindCSVFiles_EmptyDirectory returns empty list for empty spool directory.
func TestFindCSVFiles_EmptyDirectory(t *testing.T) {
	// given: an empty spool directory
	spoolDir := t.TempDir()

	// when: finding CSV files
	files, err := FindCSVFiles(spoolDir)

	// then: no error and empty list returned
	assert.NoError(t, err)
	assert.Empty(t, files)
}

// TestFindCSVFiles_FindsCSVFiles verifies CSV files are found.
func TestFindCSVFiles_FindsCSVFiles(t *testing.T) {
	// given: a spool directory with some CSV and non-CSV files
	spoolDir := t.TempDir()

	// Create test files
	csvFile1 := filepath.Join(spoolDir, "events.csv")
	csvFile2 := filepath.Join(spoolDir, "users.csv")
	metaFile := filepath.Join(spoolDir, "events.meta.json")
	txtFile := filepath.Join(spoolDir, "readme.txt")

	err := os.WriteFile(csvFile1, []byte("data"), 0o644)
	assert.NoError(t, err)
	err = os.WriteFile(csvFile2, []byte("data"), 0o644)
	assert.NoError(t, err)
	err = os.WriteFile(metaFile, []byte("meta"), 0o644)
	assert.NoError(t, err)
	err = os.WriteFile(txtFile, []byte("text"), 0o644)
	assert.NoError(t, err)

	// when: finding CSV files
	files, err := FindCSVFiles(spoolDir)

	// then: only CSV files are returned (in any order)
	assert.NoError(t, err)
	assert.Len(t, files, 2)

	// Convert to map for easier comparison (order-independent)
	fileMap := make(map[string]bool)
	for _, f := range files {
		fileMap[f] = true
	}
	assert.True(t, fileMap[csvFile1])
	assert.True(t, fileMap[csvFile2])
	assert.False(t, fileMap[metaFile])
	assert.False(t, fileMap[txtFile])
}

// TestFindCSVFiles_IgnoresDirectories verifies directories are not returned.
func TestFindCSVFiles_IgnoresDirectories(t *testing.T) {
	// given: a spool directory with subdirectories and CSV files
	spoolDir := t.TempDir()

	// Create test files and directories
	csvFile := filepath.Join(spoolDir, "events.csv")
	subDir := filepath.Join(spoolDir, "subdir")

	err := os.WriteFile(csvFile, []byte("data"), 0o644)
	assert.NoError(t, err)
	err = os.Mkdir(subDir, 0o750)
	assert.NoError(t, err)

	// when: finding CSV files
	files, err := FindCSVFiles(spoolDir)

	// then: only CSV files returned, not directories
	assert.NoError(t, err)
	assert.Len(t, files, 1)
	assert.Equal(t, csvFile, files[0])
}

// TestFindCSVFiles_NonexistentDirectory returns error.
func TestFindCSVFiles_NonexistentDirectory(t *testing.T) {
	// given: a nonexistent directory
	nonexistentDir := "/nonexistent/directory"

	// when: finding CSV files
	_, err := FindCSVFiles(nonexistentDir)

	// then: error is returned
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reading spool directory")
}

// TestGetMetadataPathForCSV_GeneratesCorrectPath verifies metadata path generation.
func TestGetMetadataPathForCSV_GeneratesCorrectPath(t *testing.T) {
	tests := []struct {
		name         string
		csvPath      string
		expectedMeta string
	}{
		{
			name:         "simple case",
			csvPath:      "/spool/events.csv",
			expectedMeta: "/spool/events.meta.json",
		},
		{
			name:         "with underscores",
			csvPath:      "/spool/a3b5c7f9e1d4b2a6_user_events.csv",
			expectedMeta: "/spool/a3b5c7f9e1d4b2a6_user_events.meta.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when: getting metadata path
			metaPath := GetMetadataPathForCSV(tt.csvPath)

			// then: path is correct
			assert.Equal(t, tt.expectedMeta, metaPath)
		})
	}
}

// TestGetMetadataPathForCSV_PreservesCWD verifies directory is preserved.
func TestGetMetadataPathForCSV_PreservesCWD(t *testing.T) {
	// given: a CSV path with multiple directory levels
	csvPath := filepath.Join("data", "spool", "fingerprint_table.csv")

	// when: getting metadata path
	metaPath := GetMetadataPathForCSV(csvPath)

	// then: directory structure is preserved
	expectedMeta := filepath.Join("data", "spool", "fingerprint_table.meta.json")
	assert.Equal(t, expectedMeta, metaPath)
}

// TestSaveAndLoadMetadata_RoundTrip verifies save-load cycle.
func TestSaveAndLoadMetadata_RoundTrip(t *testing.T) {
	// given: a temporary spool directory
	spoolDir := t.TempDir()

	// when: saving metadata file
	metaPath, err := SaveMetadataFile(spoolDir, testTableName, "a3b5c7f9e1d4b2a6", "base64-schema")
	assert.NoError(t, err)

	// and loading it back
	metadata, err := LoadMetadataFile(metaPath)
	assert.NoError(t, err)

	// then: all fields are preserved
	assert.Equal(t, testTableName, metadata.Table)
	assert.Equal(t, "a3b5c7f9e1d4b2a6", metadata.Fingerprint)
	assert.Equal(t, "base64-schema", metadata.Schema)
	assert.NotEmpty(t, metadata.CreatedAt)
}
