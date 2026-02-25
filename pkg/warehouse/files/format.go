package files

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Format defines how data is serialized to/from files.
type Format interface {
	Extension() string
	Write(w io.Writer, schema *arrow.Schema, rows []map[string]any) error
	Read(r io.Reader) (*arrow.Schema, []map[string]any, error)
}

// FormatOption configures a Format implementation.
type FormatOption func(config *formatConfig)

// formatConfig holds configuration for format implementations.
type formatConfig struct {
	compression string
}

// WithCompression configures compression for the format.
func WithCompression(compressionType string) FormatOption {
	return func(config *formatConfig) {
		config.compression = compressionType
	}
}

type csvFormat struct {
	formatConfig
}

// NewCSVFormat creates a new CSV format implementation.
func NewCSVFormat(opts ...FormatOption) Format {
	cfg := formatConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &csvFormat{formatConfig: cfg}
}

func (f *csvFormat) Extension() string {
	return "csv"
}

func (f *csvFormat) Write(w io.Writer, schema *arrow.Schema, rows []map[string]any) error {
	writer := csv.NewWriter(w)
	defer writer.Flush()

	// Write header only for empty files
	// For os.File: check size via Stat()
	// For other writers (e.g., bytes.Buffer in tests): assume empty and write header
	shouldWriteHeader := true
	if file, ok := w.(*os.File); ok {
		info, err := file.Stat()
		if err == nil && info.Size() > 0 {
			shouldWriteHeader = false
		}
	}

	if shouldWriteHeader {
		header := make([]string, len(schema.Fields()))
		for i, field := range schema.Fields() {
			header[i] = field.Name
		}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("writing CSV header: %w", err)
		}
	}

	// Write data rows
	for _, row := range rows {
		record := make([]string, len(schema.Fields()))
		for i, field := range schema.Fields() {
			val := row[field.Name]
			strVal, err := valueToString(val, field.Type)
			if err != nil {
				return fmt.Errorf("converting value for field %s: %w", field.Name, err)
			}
			record[i] = strVal
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("writing CSV row: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flushing CSV writer: %w", err)
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
		return v.Format(time.RFC3339), nil
	}

	// Handle complex types by JSON-encoding
	// This includes lists, structs, maps, and other nested types
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return "", fmt.Errorf("JSON encoding complex value: %w", err)
	}
	return string(jsonBytes), nil
}

func (f *csvFormat) Read(r io.Reader) (*arrow.Schema, []map[string]any, error) {
	return nil, nil, errors.New("CSV format not implemented")
}

type parquetFormat struct {
	formatConfig
}

// NewParquetFormat creates a new Parquet format implementation.
// Accepts optional configuration via FormatOption functions.
func NewParquetFormat(opts ...FormatOption) Format {
	cfg := formatConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &parquetFormat{formatConfig: cfg}
}

func (f *parquetFormat) Extension() string {
	return "parquet"
}

func (f *parquetFormat) Write(w io.Writer, schema *arrow.Schema, rows []map[string]any) error {
	return errors.New("parquet format not implemented")
}

func (f *parquetFormat) Read(r io.Reader) (*arrow.Schema, []map[string]any, error) {
	return nil, nil, errors.New("parquet format not implemented")
}
