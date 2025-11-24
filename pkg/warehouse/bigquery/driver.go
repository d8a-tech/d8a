// Package bigquery provides implementation of BigQuery data warehouse
package bigquery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/sirupsen/logrus"
)

type bigQueryTableDriver struct {
	db                   *bigquery.Client
	dataset              string
	queryTimeout         time.Duration
	tableCreationTimeout time.Duration
	fieldTypeMapper      warehouse.FieldTypeMapper[SpecificBigQueryType]
	typeComparer         *warehouse.TypeComparer
	writer               Writer
}

// NewBigQueryTableDriver creates a new BigQuery table driver.
func NewBigQueryTableDriver(
	db *bigquery.Client, dataset string,
	writer Writer,
	tableCreationTimeout time.Duration,
) warehouse.Driver {
	return &bigQueryTableDriver{
		db:                   db,
		dataset:              dataset,
		fieldTypeMapper:      NewFieldTypeMapper(),
		queryTimeout:         30 * time.Second,
		tableCreationTimeout: tableCreationTimeout,
		writer:               writer,
		typeComparer: warehouse.NewTypeComparer(
			int32Int64CompatibilityRule,
			float32Float64CompatibilityRule,
		),
	}
}

func (d *bigQueryTableDriver) CreateTable(table string, schema *arrow.Schema) error {
	metadata := &bigquery.TableMetadata{
		Schema: bigquery.Schema{},
	}

	for _, field := range schema.Fields() {
		fieldSchema, err := d.fieldTypeMapper.ArrowToWarehouse(
			warehouse.ArrowType{
				ArrowDataType: field.Type,
				Nullable:      field.Nullable,
			},
		)
		if err != nil {
			return err
		}
		metadata.Schema = append(metadata.Schema, &bigquery.FieldSchema{
			Name:     field.Name,
			Type:     fieldSchema.FieldType,
			Required: fieldSchema.Required,
			Repeated: fieldSchema.Repeated,
			Schema: func() bigquery.Schema {
				if fieldSchema.Schema == nil {
					return nil
				}
				return *fieldSchema.Schema
			}(),
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.queryTimeout)
	defer cancel()
	err := d.db.Dataset(d.dataset).Table(table).Create(ctx, metadata)
	if err != nil {
		if isAlreadyExistsErr(err) {
			return warehouse.NewTableAlreadyExistsError(fmt.Sprintf("%s.%s", d.dataset, table))
		}
		return err
	}

	// Verify table creation with configurable timeout
	if err := d.waitForTableCreation(table); err != nil {
		return fmt.Errorf("table creation verification failed: %w", err)
	}

	return nil
}

// waitForTableCreation polls BigQuery to verify that the table was created successfully
func (d *bigQueryTableDriver) waitForTableCreation(table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.tableCreationTimeout)
	defer cancel()

	tableRef := d.db.Dataset(d.dataset).Table(table)
	ticker := time.NewTicker(500 * time.Millisecond) // Poll every 500ms
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for table %s.%s to be created after %v", d.dataset, table, d.tableCreationTimeout)
		case <-ticker.C:
			// Check if table exists by trying to get its metadata
			metadataCtx, metadataCancel := context.WithTimeout(context.Background(), d.queryTimeout)
			_, err := tableRef.Metadata(metadataCtx)
			metadataCancel()

			if err == nil {
				// Table exists and metadata is accessible
				return nil
			}
			logrus.Errorf("error checking table existence: %v", err)

			// If it's not a "not found" error, something else went wrong
			if !isNotFoundErr(err) {
				return fmt.Errorf("error checking table existence: %w", err)
			}
			// Continue polling if it's a "not found" error
		}
	}
}

func (d *bigQueryTableDriver) AddColumn(table string, field *arrow.Field) error {
	// First check if column already exists by getting table metadata
	ctx, cancel := context.WithTimeout(context.Background(), d.queryTimeout)
	defer cancel()

	tableRef := d.db.Dataset(d.dataset).Table(table)
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		// Check if this is a "not found" error
		if isNotFoundErr(err) {
			return warehouse.NewTableNotFoundError(fmt.Sprintf("%s.%s", d.dataset, table))
		}
		return fmt.Errorf("error getting table metadata: %w", err)
	}

	// Check if column already exists
	for _, existingField := range metadata.Schema {
		if existingField.Name == field.Name {
			return warehouse.NewColumnAlreadyExistsError(fmt.Sprintf("%s.%s", d.dataset, table), field.Name)
		}
	}

	// Convert Arrow field to BigQuery field schema
	fieldSchema, err := d.fieldTypeMapper.ArrowToWarehouse(
		warehouse.ArrowType{
			ArrowDataType: field.Type,
			Nullable:      field.Nullable,
		},
	)
	if err != nil {
		return fmt.Errorf("error converting field type: %w", err)
	}

	// Create new BigQuery field schema
	newBQField := &bigquery.FieldSchema{
		Name:     field.Name,
		Type:     fieldSchema.FieldType,
		Required: fieldSchema.Required,
		Repeated: fieldSchema.Repeated,
		Schema: func() bigquery.Schema {
			if fieldSchema.Schema == nil {
				return nil
			}
			return *fieldSchema.Schema
		}(),
	}

	// Update table schema by adding the new field
	updatedSchema := make(bigquery.Schema, len(metadata.Schema)+1)
	copy(updatedSchema, metadata.Schema)
	updatedSchema[len(metadata.Schema)] = newBQField

	// Create updated metadata
	updatedMetadata := bigquery.TableMetadataToUpdate{
		Schema: updatedSchema,
	}

	// Execute the schema update
	_, err = tableRef.Update(ctx, updatedMetadata, metadata.ETag)
	if err != nil {
		// Check if this is a duplicate column error from BigQuery
		if isAlreadyExistsErr(err) {
			return warehouse.NewColumnAlreadyExistsError(fmt.Sprintf("%s.%s", d.dataset, table), field.Name)
		}
		return fmt.Errorf("error adding column: %w", err)
	}

	return nil
}

func (d *bigQueryTableDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	// Get existing table schema
	ctx, cancel := context.WithTimeout(context.Background(), d.queryTimeout)
	defer cancel()

	tableRef := d.db.Dataset(d.dataset).Table(table)
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		// Check if this is a "not found" error
		if isNotFoundErr(err) {
			return nil, warehouse.NewTableNotFoundError(fmt.Sprintf("%s.%s", d.dataset, table))
		}
		return nil, err
	}

	// Convert BigQuery schema to map for efficient lookup
	existingFields := make(map[string]*arrow.Field)
	for _, bqField := range metadata.Schema {
		arrowField, err := d.fieldTypeMapper.WarehouseToArrow(SpecificBigQueryType{
			FieldType: bqField.Type,
			Required:  bqField.Required,
			Repeated:  bqField.Repeated,
			Schema:    &bqField.Schema,
		})
		if err != nil {
			return nil, err
		}
		existingFields[bqField.Name] = &arrow.Field{
			Name:     bqField.Name,
			Type:     arrowField.ArrowDataType,
			Nullable: arrowField.Nullable,
		}
	}

	// Use common function from warehouse/diff.go
	tableName := fmt.Sprintf("%s.%s", d.dataset, table)
	return warehouse.FindMissingColumns(tableName, existingFields, schema, d)
}

// AreFieldsCompatible implements warehouse.FieldCompatibilityChecker
func (d *bigQueryTableDriver) AreFieldsCompatible(existing, input *arrow.Field) (bool, error) {
	return d.areFieldsCompatible(existing, input)
}

// areFieldsCompatible checks if two Arrow fields are compatible in BigQuery context
// This includes both type compatibility and nullability compatibility
func (d *bigQueryTableDriver) areFieldsCompatible(existing, input *arrow.Field) (bool, error) {
	if existing.Nullable != input.Nullable {
		return false, fmt.Errorf("nullability differs - existing: %t, input: %t", existing.Nullable, input.Nullable)
	}

	result := d.typeComparer.Compare(existing.Type, input.Type, existing.Name)
	if !result.Equal {
		return false, errors.New(result.ErrorMessage)
	}
	return true, nil
}

func (d *bigQueryTableDriver) Write(
	ctx context.Context,
	table string,
	schema *arrow.Schema,
	rows []map[string]any,
) error {
	return d.writer.Write(ctx, table, schema, rows)
}

type dynamicRowSaver struct {
	Data map[string]any
}

// Save implements bigquery.ValueSaver interface
func (d *dynamicRowSaver) Save() (values map[string]bigquery.Value, insertID string, err error) {
	// Convert map[string]any to map[string]bigquery.Value
	row := make(map[string]bigquery.Value)
	for k, v := range d.Data {
		row[k] = v
	}
	// Return the row data, insertID (empty for auto-generation), and no error
	return row, "", nil
}

func int32Int64CompatibilityRule(expected, actual arrow.DataType) (compatible, handled bool) {
	if expected.ID() == arrow.INT32 && actual.ID() == arrow.INT64 {
		return true, true
	}
	if expected.ID() == arrow.INT64 && actual.ID() == arrow.INT32 {
		return true, true
	}
	return false, false
}

func float32Float64CompatibilityRule(expected, actual arrow.DataType) (compatible, handled bool) {
	if expected.ID() == arrow.FLOAT32 && actual.ID() == arrow.FLOAT64 {
		return true, true
	}
	if expected.ID() == arrow.FLOAT64 && actual.ID() == arrow.FLOAT32 {
		return true, true
	}
	return false, false
}
