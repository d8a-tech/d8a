package dbip

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectBestMMDBFile(t *testing.T) {
	tests := []struct {
		name           string
		files          []string
		expectedResult string
		expectError    bool
	}{
		{
			name:           "single versioned file",
			files:          []string{"dbip-city-lite-2026-01.mmdb"},
			expectedResult: "dbip-city-lite-2026-01.mmdb",
			expectError:    false,
		},
		{
			name:           "multiple versioned files, select newest",
			files:          []string{"dbip-city-lite-2025-12.mmdb", "dbip-city-lite-2026-01.mmdb"},
			expectedResult: "dbip-city-lite-2026-01.mmdb",
			expectError:    false,
		},
		{
			name:           "multiple versioned files, same year different month",
			files:          []string{"dbip-city-lite-2026-01.mmdb", "dbip-city-lite-2026-02.mmdb"},
			expectedResult: "dbip-city-lite-2026-02.mmdb",
			expectError:    false,
		},
		{
			name: "versioned files with different years",
			files: []string{
				"dbip-city-lite-2024-12.mmdb",
				"dbip-city-lite-2026-01.mmdb",
				"dbip-city-lite-2025-06.mmdb",
			},
			expectedResult: "dbip-city-lite-2026-01.mmdb",
			expectError:    false,
		},
		{
			name:           "non-versioned files, select lexicographically greatest",
			files:          []string{"dbip-city-lite.mmdb", "dbip-city.mmdb", "dbip.mmdb"},
			expectedResult: "dbip.mmdb", // "." > "-" in ASCII, so "dbip.mmdb" is lexicographically greatest
			expectError:    false,
		},
		{
			name:           "mixed versioned and non-versioned, prefer versioned",
			files:          []string{"dbip-city-lite.mmdb", "dbip-city-lite-2025-12.mmdb"},
			expectedResult: "dbip-city-lite-2025-12.mmdb",
			expectError:    false,
		},
		{
			name:           "no mmdb files",
			files:          []string{"other-file.txt"},
			expectedResult: "",
			expectError:    false,
		},
		{
			name:           "empty directory",
			files:          []string{},
			expectedResult: "",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			tempDir := t.TempDir()
			for _, fileName := range tt.files {
				filePath := filepath.Join(tempDir, fileName)
				err := os.WriteFile(filePath, []byte("test content"), 0o644)
				require.NoError(t, err)
			}

			// when
			result, err := selectBestMMDBFile(tempDir, ".mmdb")

			// then
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.expectedResult == "" {
					assert.Empty(t, result)
				} else {
					expectedPath := filepath.Join(tempDir, tt.expectedResult)
					assert.Equal(t, expectedPath, result)
				}
			}
		})
	}
}

func TestSelectBestMMDBFile_MissingDirectory_ReturnsNotExist(t *testing.T) {
	_, err := selectBestMMDBFile(filepath.Join(t.TempDir(), "missing"), ".mmdb")

	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}
