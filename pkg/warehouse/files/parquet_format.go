package files

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

type parquetFormat struct {
	compressionCodec compress.Codec
}

// ParquetFormatOption configures parquet format behavior.
type ParquetFormatOption func(*parquetFormat)

// WithParquetCompression sets parquet compression codec.
func WithParquetCompression(codec compress.Codec) ParquetFormatOption {
	return func(f *parquetFormat) {
		f.compressionCodec = codec
	}
}

// NewParquetFormat creates a parquet format implementation.
func NewParquetFormat(opts ...ParquetFormatOption) Format {
	format := &parquetFormat{}
	for _, opt := range opts {
		opt(format)
	}

	return format
}

func (f *parquetFormat) Extension() string {
	return "parquet"
}

func (f *parquetFormat) NewWriter(w io.Writer, schema *arrow.Schema) (FormatWriter, error) {
	mapper := NewParquetFieldTypeMapper()
	pqSchema, formatFuncs, err := buildParquetSchema(schema, mapper)
	if err != nil {
		return nil, fmt.Errorf("building parquet schema: %w", err)
	}

	writerOpts := []parquet.WriterOption{pqSchema}
	if f.compressionCodec != nil {
		writerOpts = append(writerOpts, parquet.Compression(f.compressionCodec))
	}

	return &parquetFormatWriter{
		writer:      parquet.NewGenericWriter[map[string]any](w, writerOpts...),
		schema:      schema,
		formatFuncs: formatFuncs,
	}, nil
}

type parquetFormatWriter struct {
	writer      *parquet.GenericWriter[map[string]any]
	schema      *arrow.Schema
	formatFuncs []func(any, arrow.Metadata) (any, error)
	closed      bool
}

func (w *parquetFormatWriter) WriteRows(rows []map[string]any) error {
	if w.closed {
		return errors.New("format writer is closed")
	}

	formattedRows := make([]map[string]any, len(rows))
	for rowIdx, row := range rows {
		formattedRow := make(map[string]any, len(w.schema.Fields()))
		for fieldIdx, field := range w.schema.Fields() {
			formattedValue, err := w.formatFuncs[fieldIdx](row[field.Name], field.Metadata)
			if err != nil {
				return fmt.Errorf("formatting value for field %s: %w", field.Name, err)
			}
			formattedRow[field.Name] = normalizeParquetValue(formattedValue)
		}
		formattedRows[rowIdx] = formattedRow
	}

	if _, err := w.writer.Write(formattedRows); err != nil {
		return fmt.Errorf("writing parquet rows: %w", err)
	}

	return nil
}

func normalizeParquetValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		normalized := make(map[string]any, len(v))
		for key, nested := range v {
			normalized[key] = normalizeParquetValue(nested)
		}
		return normalized
	case []any:
		allMaps := true
		mapSlice := make([]map[string]any, len(v))
		for idx, nested := range v {
			normalizedNested := normalizeParquetValue(nested)
			nestedMap, ok := normalizedNested.(map[string]any)
			if !ok {
				allMaps = false
				break
			}
			mapSlice[idx] = nestedMap
		}

		if allMaps {
			return mapSlice
		}

		normalized := make([]any, len(v))
		for idx, nested := range v {
			normalized[idx] = normalizeParquetValue(nested)
		}
		return normalized
	default:
		return value
	}
}

func (w *parquetFormatWriter) Close() error {
	if w.closed {
		return nil
	}

	w.closed = true
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("closing parquet writer: %w", err)
	}

	return nil
}
