package files

import (
	"bytes"
	"crypto/sha256"
	"fmt"
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

// parsePathTemplate parses and validates a path template string.
// It returns an error if the template is empty, has invalid syntax,
// or contains path traversal sequences (..).
func parsePathTemplate(tmplStr string) (*template.Template, error) {
	tmplStr = strings.TrimSpace(tmplStr)
	if tmplStr == "" {
		return nil, fmt.Errorf("template string cannot be empty")
	}

	tmpl, err := template.New("path").Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	// Validate template doesn't produce path traversal by executing with sample data
	sampleData := struct {
		Table       string
		Schema      string
		SegmentID   string
		Extension   string
		Year        int
		Month       int
		MonthPadded string
		Day         int
		DayPadded   string
	}{
		Table:       "test",
		Schema:      "abc123",
		SegmentID:   "12345_uuid",
		Extension:   "csv",
		Year:        2026,
		Month:       3,
		MonthPadded: "03",
		Day:         1,
		DayPadded:   "01",
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, sampleData); err != nil {
		return nil, fmt.Errorf("executing template with sample data: %w", err)
	}

	if strings.Contains(buf.String(), "..") {
		return nil, fmt.Errorf("template output contains path traversal sequence (..)")
	}

	return tmpl, nil
}
