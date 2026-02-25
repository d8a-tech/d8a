package files

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// schemaFingerprint returns a 16-character SHA256-based fingerprint for the schema.
func schemaFingerprint(schema *arrow.Schema) string {
	arrowFp := schema.Fingerprint()
	hash := sha256.Sum256([]byte(arrowFp))
	hexStr := fmt.Sprintf("%x", hash)
	if len(hexStr) > 16 {
		return hexStr[:16]
	}
	return hexStr
}

// escapeTableName replaces unsafe characters with underscores.
func escapeTableName(table string) string {
	if table == "" {
		return table
	}

	var builder strings.Builder
	builder.Grow(len(table))
	for _, ch := range table {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			builder.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		case ch == '-' || ch == '_':
			builder.WriteRune(ch)
		default:
			builder.WriteRune('_')
		}
	}

	return builder.String()
}

// streamDir returns the stream directory for a table+schema fingerprint.
func streamDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(spoolDir, "streams", tableEsc, fingerprint)
}

// activePath returns the active segment path for a stream.
func activePath(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(streamDir(spoolDir, tableEsc, fingerprint), "active.csv")
}

// sealedDir returns the sealed segments directory for a stream.
func sealedDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(streamDir(spoolDir, tableEsc, fingerprint), "sealed")
}

// uploadingDir returns the uploading segments directory for a stream.
func uploadingDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(streamDir(spoolDir, tableEsc, fingerprint), "uploading")
}

// failedDir returns the failed segments directory for a stream.
func failedDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(streamDir(spoolDir, tableEsc, fingerprint), "failed")
}

// segmentPath returns the path for a segment ID within a directory.
func segmentPath(dir, segmentID, ext string) string {
	return filepath.Join(dir, fmt.Sprintf("%s.%s", segmentID, ext))
}

// failCountPath returns the path for a segment fail counter within the stream directory.
func failCountPath(streamDir, segmentID string) string {
	return filepath.Join(streamDir, fmt.Sprintf("%s.failcount", segmentID))
}

// segmentRemoteKey returns the remote object key for a segment.
func segmentRemoteKey(tableEsc, fingerprint, segmentID, ext string, sealTime time.Time) string {
	date := sealTime.UTC().Format("2006/01/02")
	return fmt.Sprintf("table=%s/schema=%s/dt=%s/%s.%s", tableEsc, fingerprint, date, segmentID, ext)
}

// ensureStreamDirs creates the directory structure for a stream.
func ensureStreamDirs(spoolDir, tableEsc, fingerprint string) error {
	paths := []string{
		streamDir(spoolDir, tableEsc, fingerprint),
		sealedDir(spoolDir, tableEsc, fingerprint),
		uploadingDir(spoolDir, tableEsc, fingerprint),
		failedDir(spoolDir, tableEsc, fingerprint),
	}

	for _, path := range paths {
		if err := os.MkdirAll(path, 0o750); err != nil {
			return fmt.Errorf("creating stream dir %s: %w", path, err)
		}
	}

	return nil
}

func findSealedSegments(sealedDir, ext string) ([]string, error) {
	entries, err := os.ReadDir(sealedDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("reading sealed dir %s: %w", sealedDir, err)
	}

	dotExt := "." + ext
	segments := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != dotExt {
			continue
		}
		segments = append(segments, strings.TrimSuffix(name, dotExt))
	}

	return segments, nil
}
