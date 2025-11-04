// Package clickhouse provides implementation of Clickhouse data warehouse
package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/sirupsen/logrus"
)

type clickhouseDriver struct {
	db              *sql.DB
	conn            clickhouse.Conn
	database        string
	queryMapper     warehouse.QueryMapper
	fieldTypeMapper warehouse.FieldTypeMapper[SpecificClickhouseType]
	queryTimeout    time.Duration
	typeComparer    *warehouse.TypeComparer
}

// NewClickHouseTableDriver creates a new ClickHouse table driver.
func NewClickHouseTableDriver(chOptions *clickhouse.Options, database string, opts ...Options) warehouse.Driver {
	chOptions.Settings["flatten_nested"] = false
	db := clickhouse.OpenDB(chOptions)
	conn, err := clickhouse.Open(chOptions)
	if err != nil {
		logrus.Fatalf("Failed to open ClickHouse connection: %v", err)
	}

	return &clickhouseDriver{
		db:              db,
		conn:            conn,
		database:        database,
		queryMapper:     NewClickHouseQueryMapper(opts...),
		fieldTypeMapper: NewFieldTypeMapper(),
		queryTimeout:    30 * time.Second,
		typeComparer:    warehouse.NewTypeComparer(),
	}
}

func (d *clickhouseDriver) CreateTable(table string, schema *arrow.Schema) error {
	// Construct full table name with database
	fullTableName := fmt.Sprintf("%s.%s", d.database, table)
	query, err := warehouse.CreateTableQuery(d.queryMapper, fullTableName, schema)
	if err != nil {
		return err
	}

	_, err = d.db.Exec(query)
	if err != nil {
		if isAlreadyExistsErr(err) {
			return warehouse.NewTableAlreadyExistsError(fullTableName)
		}
		return err
	}

	return nil
}

func (d *clickhouseDriver) AddColumn(table string, field *arrow.Field) error {
	// First check if column already exists by querying system.columns
	ctx, cancel := context.WithTimeout(context.Background(), d.queryTimeout)
	defer cancel()

	checkQuery := `
		SELECT COUNT(*) 
		FROM system.columns 
		WHERE database = ? AND table = ? AND name = ?
	`

	var count int
	err := d.db.QueryRowContext(ctx, checkQuery, d.database, table, field.Name).Scan(&count)
	if err != nil {
		return fmt.Errorf("error checking if column exists: %w", err)
	}

	// If column already exists, return appropriate error
	if count > 0 {
		return warehouse.NewColumnAlreadyExistsError(fmt.Sprintf("%s.%s", d.database, table), field.Name)
	}

	// Convert Arrow field to ClickHouse column type
	columnType, err := d.queryMapper.Field(field)
	if err != nil {
		return fmt.Errorf("error converting field type: %w", err)
	}

	// Build ALTER TABLE ADD COLUMN query
	fullTableName := fmt.Sprintf("%s.%s", d.database, table)
	alterQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", fullTableName, field.Name, columnType)

	// Execute the ALTER TABLE query
	_, err = d.db.ExecContext(ctx, alterQuery)
	if err != nil {
		// Check if this is a duplicate column error from ClickHouse
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "duplicate") {
			return warehouse.NewColumnAlreadyExistsError(fullTableName, field.Name)
		}
		return fmt.Errorf("error adding column: %w", err)
	}

	return nil
}

func (d *clickhouseDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	// Query ClickHouse system.columns table to get existing column information
	ctx, cancel := context.WithTimeout(context.Background(), d.queryTimeout)
	defer cancel()

	query := `
		SELECT name, type 
		FROM system.columns 
		WHERE database = ? AND table = ?
		ORDER BY name
	`

	rows, err := d.db.QueryContext(ctx, query, d.database, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logrus.Error("Failed to close database rows: ", err)
		}
	}()

	// Check if table exists by checking if we got any rows
	existingFields := make(map[string]*arrow.Field)
	hasRows := false

	for rows.Next() {
		hasRows = true
		var columnName, columnType string
		if err := rows.Scan(&columnName, &columnType); err != nil {
			return nil, err
		}

		// Convert ClickHouse system.columns type to Arrow type using proper type mapper
		arrowType, err := d.fieldTypeMapper.WarehouseToArrow(anyType(columnType))
		if err != nil {
			return nil, err
		}

		// Create Arrow field
		arrowField := &arrow.Field{
			Name:     columnName,
			Type:     arrowType.ArrowDataType,
			Nullable: arrowType.Nullable,
			Metadata: arrowType.Metadata,
		}
		existingFields[columnName] = arrowField
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// If no rows were returned, the table doesn't exist
	if !hasRows {
		return nil, warehouse.NewTableNotFoundError(fmt.Sprintf("%s.%s", d.database, table))
	}

	// Use common function from warehouse/diff.go
	tableName := fmt.Sprintf("%s.%s", d.database, table)
	return warehouse.FindMissingColumns(tableName, existingFields, schema, d)
}

func (d *clickhouseDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	// given
	if len(rows) == 0 {
		return nil
	}

	// Construct full table name with database
	fullTableName := fmt.Sprintf("%s.%s", d.database, table)

	// Get column names and types from schema
	columns := make([]string, len(schema.Fields()))
	columnTypes := make([]SpecificClickhouseType, len(schema.Fields()))
	for i, field := range schema.Fields() {
		columns[i] = field.Name
		arrowType := warehouse.ArrowType{
			ArrowDataType: field.Type,
			Nullable:      field.Nullable,
			Metadata:      field.Metadata,
		}
		chType, err := d.fieldTypeMapper.ArrowToWarehouse(arrowType)
		if err != nil {
			return fmt.Errorf("error mapping type for column %s: %w", field.Name, err)
		}
		columnTypes[i] = chType
	}

	// Create batch
	batch, err := d.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", fullTableName))
	if err != nil {
		return fmt.Errorf("error preparing batch: %w", err)
	}
	defer func() {
		if err := batch.Send(); err != nil {
			logrus.Error("Failed to send batch: ", err)
		}
	}()

	// Process each row
	for _, row := range rows {
		// Prepare row values
		values := make([]any, len(columns))
		for i, col := range columns {
			value, exists := row[col]
			if !exists {
				return fmt.Errorf("missing value for column %s", col)
			}

			// Format value according to column type
			formattedValue, err := columnTypes[i].Format(value, schema.Field(i).Metadata)
			if err != nil {
				return fmt.Errorf("error formatting value for column %s: %w", col, err)
			}
			values[i] = formattedValue
		}

		// Append row to batch
		if err := batch.Append(values...); err != nil {
			return fmt.Errorf("error appending row to batch: %w", err)
		}
	}

	return nil
}

// AreFieldsCompatible implements warehouse.FieldCompatibilityChecker
func (d *clickhouseDriver) AreFieldsCompatible(existing, input *arrow.Field) (bool, error) {
	return d.areFieldsCompatible(existing, input)
}

// areFieldsCompatible checks if two Arrow types are compatible in ClickHouse context
// ClickHouse requires strict type matching (unlike BigQuery's flexible rules)
func (d *clickhouseDriver) areFieldsCompatible(existing, input *arrow.Field) (bool, error) {
	// For array types (List types), ignore nullability differences since ClickHouse arrays are always non-nullable
	_, existingIsArray := existing.Type.(*arrow.ListType)
	_, inputIsArray := input.Type.(*arrow.ListType)

	if !existingIsArray && !inputIsArray && existing.Nullable != input.Nullable {
		return false, fmt.Errorf("nullability differs - existing: %t, input: %t", existing.Nullable, input.Nullable)
	}

	result := d.typeComparer.Compare(existing.Type, input.Type, existing.Name)
	if !result.Equal {
		return false, errors.New(result.ErrorMessage)
	}
	return true, nil
}
