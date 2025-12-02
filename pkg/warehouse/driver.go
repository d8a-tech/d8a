// Package warehouse provides a set of interfaces and implementations for working with data warehouses.
package warehouse

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Registry is a registry of drivers for different properties.
type Registry interface {
	Get(propertyID string) (Driver, error)
}

// Driver abstracts data warehouse operations for table management and data ingestion.
// Implementations handle warehouse-specific DDL/DML operations while maintaining
// compatibility with Apache Arrow schemas.
type Driver interface {
	// CreateTable creates a new table with the specified Arrow schema.
	// Returns error if table exists or schema conversion fails.
	// Implementation must convert Arrow types to warehouse-native types.
	CreateTable(table string, schema *arrow.Schema) error

	// AddColumn adds a new column to an existing table.
	AddColumn(table string, field *arrow.Field) error

	// Write inserts batch data into the specified table.
	// Schema must match table structure. Rows contain column_name -> value mappings.
	// Implementation handles type conversion and batch optimization.
	// Returns error on type mismatch, constraint violation, or connection issues.
	Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error

	// MissingColumns compares provided schema against existing table structure.
	// Returns fields that exist in schema but not in table.
	// Used for schema drift detection before writes.
	// Returns TableNotFoundError if table doesn't exist.
	MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error)
}

// QueryMapper defines SQL DDL query construction from Arrow schemas.
// Used by Driver implementations that operate on sql.DB to generate
// warehouse-specific CREATE TABLE statements.
type QueryMapper interface {
	// TablePredicate returns warehouse-specific table creation prefix.
	// Examples: "TABLE", "TABLE IF NOT EXISTS", "TEMPORARY TABLE"
	TablePredicate(table string) string

	// Field converts Arrow field to warehouse column type definition.
	// Must handle type mapping, nullability, and metadata.
	// Returns string like "VARCHAR(255)", "Int64", "TIMESTAMP", etc.
	Field(*arrow.Field) (string, error)

	// TableSuffix returns warehouse-specific table creation suffix.
	// Examples: "ENGINE = MergeTree()", "OPTIONS (description='...')"
	// May include newlines for multi-line clauses.
	TableSuffix(table string) string
}

// CreateTableQuery creates a DDL query to create a table from an Arrow schema, using a query mapper.
func CreateTableQuery(qm QueryMapper, table string, schema *arrow.Schema) (string, error) {
	buf := bytes.NewBufferString("CREATE ")
	buf.WriteString(qm.TablePredicate(table))
	buf.WriteString(" (\n")

	for i, field := range schema.Fields() {
		fieldType, err := qm.Field(&field)
		if err != nil {
			return "", err
		}

		buf.WriteString("  ")
		buf.WriteString(field.Name)
		buf.WriteString(" ")
		buf.WriteString(fieldType)
		if i < len(schema.Fields())-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(")")

	suffix := qm.TableSuffix(table)
	if suffix != "" {
		if strings.HasPrefix(suffix, "\n") || strings.HasPrefix(suffix, "\b\n") {
			// Remove the backspace if present and don't add space
			suffix = strings.TrimPrefix(suffix, "\b")
			buf.WriteString(suffix)
		} else {
			buf.WriteString(" ")
			buf.WriteString(suffix)
		}
	}

	return buf.String(), nil
}

type staticDriverRegistry struct {
	driver Driver
}

func (r *staticDriverRegistry) Get(_ string) (Driver, error) {
	return r.driver, nil
}

// NewStaticDriverRegistry creates a new static driver registry that always returns the same driver.
func NewStaticDriverRegistry(driver Driver) Registry {
	return &staticDriverRegistry{
		driver: NewLoggingDriver(driver),
	}
}

// NewStaticBatchedDriverRegistry creates a new static driver registry that always returns the same driver.
func NewStaticBatchedDriverRegistry(ctx context.Context, driver Driver) Registry {
	return &staticDriverRegistry{
		driver: NewBatchingDriver(ctx, driver, 5000, 1*time.Second),
	}
}
