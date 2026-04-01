package files

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

func marshalSchema(schema *arrow.Schema) ([]byte, error) {
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(rec); err != nil {
		return nil, fmt.Errorf("writing IPC record: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("closing IPC writer: %w", err)
	}

	return buf.Bytes(), nil
}

func unmarshalSchema(data []byte) (*arrow.Schema, error) {
	r, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("creating IPC reader: %w", err)
	}
	defer r.Release()

	schema := r.Schema()
	if schema == nil {
		return nil, fmt.Errorf("reading IPC schema: nil schema returned")
	}
	if r.Err() != nil {
		return nil, fmt.Errorf("reading IPC schema: %w", r.Err())
	}
	return schema, nil
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
