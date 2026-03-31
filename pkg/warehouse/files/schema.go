package files

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
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
func activePath(spoolDir, tableEsc, fingerprint, ext string) string {
	return filepath.Join(streamDir(spoolDir, tableEsc, fingerprint), "active."+ext)
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

// pathTemplateData holds the data available for path template execution.
type pathTemplateData struct {
	Table       string
	Schema      string
	SegmentID   string
	Extension   string
	Year        int
	Month       int
	MonthPadded string
	Day         int
	DayPadded   string
}

// segmentRemoteKey returns the remote object key for a segment.
func segmentRemoteKey(
	tmpl *template.Template,
	tableEsc, fingerprint, segmentID, ext string,
	sealTime time.Time,
) (string, error) {
	utc := sealTime.UTC()
	year, month, day := utc.Date()

	data := pathTemplateData{
		Table:       tableEsc,
		Schema:      fingerprint,
		SegmentID:   segmentID,
		Extension:   ext,
		Year:        year,
		Month:       int(month),
		MonthPadded: fmt.Sprintf("%02d", month),
		Day:         day,
		DayPadded:   fmt.Sprintf("%02d", day),
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("executing path template: %w", err)
	}

	return buf.String(), nil
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
		if !strings.HasSuffix(name, dotExt) {
			continue
		}
		segments = append(segments, strings.TrimSuffix(name, dotExt))
	}

	return segments, nil
}
