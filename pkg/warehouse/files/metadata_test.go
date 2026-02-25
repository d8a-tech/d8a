package files

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnsureStreamDirs(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	tableEsc := "events"
	fingerprint := "abc123"

	// when
	err := EnsureStreamDirs(spoolDir, tableEsc, fingerprint)

	// then
	assert.NoError(t, err)
	assert.DirExists(t, StreamDir(spoolDir, tableEsc, fingerprint))
	assert.DirExists(t, SealedDir(spoolDir, tableEsc, fingerprint))
	assert.DirExists(t, UploadingDir(spoolDir, tableEsc, fingerprint))
	assert.DirExists(t, FailedDir(spoolDir, tableEsc, fingerprint))
}

func TestSaveMetadataFile_CreatesFile(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	metaPath := filepath.Join(spoolDir, "active.meta.json")
	meta := &Metadata{
		Table:       "events",
		Fingerprint: "a3b5c7f9e1d4b2a6",
		Schema:      "base64-schema",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	// when
	err := SaveMetadataFile(metaPath, meta)

	// then
	assert.NoError(t, err)
	assert.FileExists(t, metaPath)
}

func TestSaveMetadataFile_AtomicWrite(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	metaPath := filepath.Join(spoolDir, "active.meta.json")
	meta := &Metadata{
		Table:       "events",
		Fingerprint: "a3b5c7f9e1d4b2a6",
		Schema:      "base64-schema",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	// when
	err := SaveMetadataFile(metaPath, meta)

	// then
	assert.NoError(t, err)
	_, err = os.Stat(metaPath + ".tmp")
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestSaveMetadataFile_InvalidDirectory(t *testing.T) {
	// given
	metaPath := "/nonexistent/directory/active.meta.json"
	meta := &Metadata{
		Table:       "events",
		Fingerprint: "a3b5c7f9e1d4b2a6",
		Schema:      "base64-schema",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	// when
	err := SaveMetadataFile(metaPath, meta)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "creating metadata temp file")
}

func TestLoadMetadataFile_Success(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	metaPath := filepath.Join(spoolDir, "active.meta.json")
	meta := &Metadata{
		Table:       "events",
		Fingerprint: "a3b5c7f9e1d4b2a6",
		Schema:      "base64-schema",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	assert.NoError(t, SaveMetadataFile(metaPath, meta))

	// when
	loaded, err := LoadMetadataFile(metaPath)

	// then
	assert.NoError(t, err)
	assert.Equal(t, meta.Table, loaded.Table)
	assert.Equal(t, meta.Fingerprint, loaded.Fingerprint)
	assert.Equal(t, meta.Schema, loaded.Schema)
}

func TestLoadMetadataFile_NonexistentFile(t *testing.T) {
	// given
	nonexistentPath := "/nonexistent/metadata.json"

	// when
	_, err := LoadMetadataFile(nonexistentPath)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "opening metadata file")
}

func TestLoadMetadataFile_MalformedFile(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	metaPath := filepath.Join(spoolDir, "malformed.meta.json")
	err := os.WriteFile(metaPath, []byte("not valid json"), 0o644)
	assert.NoError(t, err)

	// when
	_, err = LoadMetadataFile(metaPath)

	// then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reading metadata file")
}

func TestDeleteMetadataFile_Success(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	metaPath := filepath.Join(spoolDir, "active.meta.json")
	meta := &Metadata{
		Table:       "events",
		Fingerprint: "a3b5c7f9e1d4b2a6",
		Schema:      "base64-schema",
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	assert.NoError(t, SaveMetadataFile(metaPath, meta))

	// when
	err := DeleteMetadataFile(metaPath)

	// then
	assert.NoError(t, err)
	_, err = os.Stat(metaPath)
	assert.True(t, os.IsNotExist(err))
}

func TestDeleteMetadataFile_NonexistentFile_NoError(t *testing.T) {
	// given
	nonexistentPath := "/nonexistent/metadata.json"

	// when
	err := DeleteMetadataFile(nonexistentPath)

	// then
	assert.NoError(t, err)
}
