package files

import (
	"errors"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
)

// Format defines how data is serialized to/from files.
type Format interface {
	// Extension returns the file extension (e.g., "csv", "parquet").
	Extension() string

	// Write serializes rows to the writer using the provided schema.
	Write(w io.Writer, schema *arrow.Schema, rows []map[string]any) error

	// Read deserializes rows from the reader, returning schema and data.
	Read(r io.Reader) (*arrow.Schema, []map[string]any, error)
}

type csvFormat struct{}

// NewCSVFormat creates a new CSV format implementation.
func NewCSVFormat() Format {
	return &csvFormat{}
}

func (f *csvFormat) Extension() string {
	return "csv"
}

func (f *csvFormat) Write(w io.Writer, schema *arrow.Schema, rows []map[string]any) error {
	return errors.New("CSV format not implemented")
}

func (f *csvFormat) Read(r io.Reader) (*arrow.Schema, []map[string]any, error) {
	return nil, nil, errors.New("CSV format not implemented")
}

type parquetFormat struct{}

// NewParquetFormat creates a new Parquet format implementation.
func NewParquetFormat() Format {
	return &parquetFormat{}
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
