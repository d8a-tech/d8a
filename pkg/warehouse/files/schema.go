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

// SchemaFingerprint returns a 16-character SHA256-based fingerprint for the schema.
func SchemaFingerprint(schema *arrow.Schema) string {
	arrowFp := schema.Fingerprint()
	hash := sha256.Sum256([]byte(arrowFp))
	hexStr := fmt.Sprintf("%x", hash)
	if len(hexStr) > 16 {
		return hexStr[:16]
	}
	return hexStr
}

// EscapeTableName replaces unsafe characters with underscores.
func EscapeTableName(table string) string {
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

// StreamDir returns the stream directory for a table+schema fingerprint.
func StreamDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(spoolDir, "streams", tableEsc, fingerprint)
}

// ActivePath returns the active segment path for a stream.
func ActivePath(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "active.csv")
}

// SealedDir returns the sealed segments directory for a stream.
func SealedDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "sealed")
}

// UploadingDir returns the uploading segments directory for a stream.
func UploadingDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "uploading")
}

// FailedDir returns the failed segments directory for a stream.
func FailedDir(spoolDir, tableEsc, fingerprint string) string {
	return filepath.Join(StreamDir(spoolDir, tableEsc, fingerprint), "failed")
}

// SegmentPath returns the path for a segment ID within a directory.
func SegmentPath(dir, segmentID string) string {
	return filepath.Join(dir, fmt.Sprintf("%s.csv", segmentID))
}

// FailCountPath returns the path for a segment fail counter within the stream directory.
func FailCountPath(streamDir, segmentID string) string {
	return filepath.Join(streamDir, fmt.Sprintf("%s.failcount", segmentID))
}

// SegmentRemoteKey returns the remote object key for a segment.
func SegmentRemoteKey(tableEsc, fingerprint, segmentID string, sealTime time.Time) string {
	date := sealTime.UTC().Format("2006/01/02")
	return fmt.Sprintf("table=%s/schema=%s/dt=%s/%s.csv", tableEsc, fingerprint, date, segmentID)
}

// EnsureStreamDirs creates the directory structure for a stream.
func EnsureStreamDirs(spoolDir, tableEsc, fingerprint string) error {
	paths := []string{
		StreamDir(spoolDir, tableEsc, fingerprint),
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
