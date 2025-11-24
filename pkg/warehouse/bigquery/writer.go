package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
)

// Writer is an interface that represents a writer for BigQuery
type Writer interface {
	Write(ctx context.Context, tableName string, schema *arrow.Schema, rows []map[string]any) error
}

type streamingWriter struct {
	db              *bigquery.Client
	dataset         string
	queryTimeout    time.Duration
	fieldTypeMapper warehouse.FieldTypeMapper[SpecificBigQueryType]
}

// NewStreamingWriter creates a new non-free tier writer, using streaming insert
func NewStreamingWriter(
	db *bigquery.Client,
	dataset string,
	queryTimeout time.Duration,
	fieldTypeMapper warehouse.FieldTypeMapper[SpecificBigQueryType],
) Writer {
	return &streamingWriter{
		db:              db,
		dataset:         dataset,
		queryTimeout:    queryTimeout,
		fieldTypeMapper: fieldTypeMapper,
	}
}

// Write implements Writer
func (w *streamingWriter) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	fields := map[string]SpecificBigQueryType{}
	for _, field := range schema.Fields() {
		fieldSchema, err := w.fieldTypeMapper.ArrowToWarehouse(
			warehouse.ArrowType{
				ArrowDataType: field.Type,
				Nullable:      field.Nullable,
			},
		)
		if err != nil {
			return err
		}
		fields[field.Name] = fieldSchema
	}
	dataToInsert := make([]bigquery.ValueSaver, 0, len(rows))
	for _, row := range rows {
		batchRow := map[string]any{}
		for _, field := range schema.Fields() {
			fieldSchema, ok := fields[field.Name]
			if !ok {
				return fmt.Errorf("field %s not found in schema", field.Name)
			}
			formatted, err := fieldSchema.Format(row[field.Name], arrow.Metadata{})
			if err != nil {
				return err
			}
			batchRow[field.Name] = formatted
		}
		dataToInsert = append(dataToInsert, &dynamicRowSaver{Data: batchRow})
	}
	ctx, cancel := context.WithTimeout(ctx, w.queryTimeout)
	defer cancel()
	tableRef := w.db.Dataset(w.dataset).Table(table)
	inserter := tableRef.Inserter()
	err := inserter.Put(ctx, dataToInsert)
	if err != nil {
		return err
	}
	return nil
}

type loadJobWriter struct {
	db              *bigquery.Client
	dataset         string
	queryTimeout    time.Duration
	fieldTypeMapper warehouse.FieldTypeMapper[SpecificBigQueryType]
}

// NewLoadJobWriter creates a new free tier compatible writer, using load jobs with NDJSON
func NewLoadJobWriter(
	db *bigquery.Client,
	dataset string,
	queryTimeout time.Duration,
	fieldTypeMapper warehouse.FieldTypeMapper[SpecificBigQueryType],
) Writer {
	return &loadJobWriter{
		db:              db,
		dataset:         dataset,
		queryTimeout:    queryTimeout,
		fieldTypeMapper: fieldTypeMapper,
	}
}

// Write implements Writer
func (w *loadJobWriter) Write(
	ctx context.Context,
	table string,
	schema *arrow.Schema,
	rows []map[string]any,
) error {
	if len(rows) == 0 {
		return nil
	}

	// Convert arrow schema to field type map
	fields := map[string]SpecificBigQueryType{}
	for _, field := range schema.Fields() {
		fieldSchema, err := w.fieldTypeMapper.ArrowToWarehouse(
			warehouse.ArrowType{
				ArrowDataType: field.Type,
				Nullable:      field.Nullable,
			},
		)
		if err != nil {
			return err
		}
		fields[field.Name] = fieldSchema
	}

	// Format data and convert to NDJSON
	buf, err := w.formatRowsToNDJSON(schema, rows, fields)
	if err != nil {
		return err
	}

	// Create load job
	ctx, cancel := context.WithTimeout(ctx, w.queryTimeout)
	defer cancel()

	tableRef := w.db.Dataset(w.dataset).Table(table)
	source := bigquery.NewReaderSource(buf)
	source.SourceFormat = bigquery.JSON

	loader := tableRef.LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteAppend

	// Run the load job
	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("error starting load job: %w", err)
	}

	// Wait for the job to complete
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error waiting for load job: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("load job failed: %w", status.Err())
	}

	return nil
}

// formatRowsToNDJSON formats rows to NDJSON format
func (w *loadJobWriter) formatRowsToNDJSON(
	schema *arrow.Schema,
	rows []map[string]any,
	fields map[string]SpecificBigQueryType,
) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	for _, row := range rows {
		formattedRow := map[string]any{}
		for _, field := range schema.Fields() {
			fieldSchema, ok := fields[field.Name]
			if !ok {
				return nil, fmt.Errorf("field %s not found in schema", field.Name)
			}
			formatted, err := fieldSchema.Format(row[field.Name], arrow.Metadata{})
			if err != nil {
				return nil, err
			}
			formattedRow[field.Name] = formatted
		}

		// Convert row to JSON and write to buffer
		jsonData, err := json.Marshal(formattedRow)
		if err != nil {
			return nil, fmt.Errorf("error marshaling row to JSON: %w", err)
		}
		buf.Write(jsonData)
		buf.WriteByte('\n')
	}

	return &buf, nil
}
