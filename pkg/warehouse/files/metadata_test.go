package files

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureStreamDirs(t *testing.T) {
	// given
	spoolDir := t.TempDir()
	tableEsc := "events"
	fingerprint := testFingerprint

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

func TestFindSealedSegments(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T, dir string)
		expected []string
	}{
		{
			name: "empty dir",
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(dir, 0o750))
			},
			expected: []string{},
		},
		{
			name: "non-existent dir",
			setup: func(t *testing.T, dir string) {
				// do nothing
			},
			expected: []string{},
		},
		{
			name: "dir with segments and other files",
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(dir, 0o750))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "seg1.csv"), []byte("id\n1"), 0o600))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "seg2.csv"), []byte("id\n2"), 0o600))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "other.txt"), []byte("ignore"), 0o600))
			},
			expected: []string{"seg1", "seg2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "sealed")
			tt.setup(t, dir)

			segments, err := findSealedSegments(dir)

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, segments)
		})
	}
}

const testFingerprint = "abc123"
