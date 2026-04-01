package files

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Format defines how data is serialized to files.
type Format interface {
	Extension() string
	NewWriter(w io.Writer, schema *arrow.Schema) (FormatWriter, error)
}

// FormatWriter writes rows for a single output stream.
type FormatWriter interface {
	WriteRows(rows []map[string]any) error
	Close() error
}

type csvFormat struct {
	compressionLevel *int
}

// CSVFormatOption configures the CSV format behavior.
type CSVFormatOption func(*csvFormat)

// WithCompression is a pass-through to keep configuration options grouped.
func WithCompression(opt CSVFormatOption) CSVFormatOption {
	return opt
}

// Gzip enables gzip compression with the provided level.
func Gzip(level int) CSVFormatOption {
	return func(f *csvFormat) {
		f.compressionLevel = &level
	}
}

// NewCSVFormat creates a new CSV format implementation.
func NewCSVFormat(opts ...CSVFormatOption) Format {
	format := &csvFormat{}
	for _, opt := range opts {
		opt(format)
	}
	return format
}

func (f *csvFormat) Extension() string {
	if f.compressionLevel != nil {
		return "csv.gz"
	}
	return "csv"
}

func (f *csvFormat) NewWriter(w io.Writer, schema *arrow.Schema) (FormatWriter, error) {
	var gz *gzip.Writer
	var writer *csv.Writer
	if f.compressionLevel == nil {
		writer = csv.NewWriter(w)
	} else {
		var err error
		gz, err = gzip.NewWriterLevel(w, *f.compressionLevel)
		if err != nil {
			return nil, fmt.Errorf("creating gzip writer: %w", err)
		}
		writer = csv.NewWriter(gz)
	}

	return &csvFormatWriter{
		schema: schema,
		writer: writer,
		gz:     gz,
	}, nil
}

type csvFormatWriter struct {
	schema        *arrow.Schema
	writer        *csv.Writer
	gz            *gzip.Writer
	headerWritten bool
	closed        bool
}

func (w *csvFormatWriter) WriteRows(rows []map[string]any) error {
	if w.closed {
		return errors.New("format writer is closed")
	}

	if !w.headerWritten {
		header := make([]string, len(w.schema.Fields()))
		for i, field := range w.schema.Fields() {
			header[i] = field.Name
		}
		if err := w.writer.Write(header); err != nil {
			return fmt.Errorf("writing CSV header: %w", err)
		}
		w.headerWritten = true
	}

	for _, row := range rows {
		record := make([]string, len(w.schema.Fields()))
		for i, field := range w.schema.Fields() {
			val := row[field.Name]
			strVal, err := valueToString(val, field.Type)
			if err != nil {
				return fmt.Errorf("converting value for field %s: %w", field.Name, err)
			}
			record[i] = strVal
		}
		if err := w.writer.Write(record); err != nil {
			return fmt.Errorf("writing CSV row: %w", err)
		}
	}

	return nil
}

func (w *csvFormatWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return fmt.Errorf("flushing CSV writer: %w", err)
	}

	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			return fmt.Errorf("closing gzip writer: %w", err)
		}
	}

	return nil
}

// valueToString converts an arbitrary value to a string for CSV output.
func valueToString(val any, fieldType arrow.DataType) (string, error) {
	// Handle nil
	if val == nil {
		return "", nil
	}

	// Handle primitives
	switch v := val.(type) {
	case string:
		return v, nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v), nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		return fmt.Sprintf("%v", v), nil
	case time.Time:
		return v.Format(time.RFC3339Nano), nil
	}

	// Handle complex types by JSON-encoding
	// This includes lists, structs, maps, and other nested types
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return "", fmt.Errorf("JSON encoding complex value: %w", err)
	}
	return string(jsonBytes), nil
}
