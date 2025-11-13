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
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/sirupsen/logrus"
)

type clickhouseDriver struct {
	db                *sql.DB
	conn              clickhouse.Conn
	database          string
	queryMapper       warehouse.QueryMapper
	fieldTypeMapper   warehouse.FieldTypeMapper[SpecificClickhouseType]
	queryTimeout      time.Duration
	typeComparer      *warehouse.TypeComparer
	tableColumnsCache *util.TTLCache[[]*arrow.Field]
}

// NewClickHouseTableDriver creates a new ClickHouse table driver.
func NewClickHouseTableDriver(chOptions *clickhouse.Options, database string, opts ...Options) warehouse.Driver {
	chOptions.Settings["flatten_nested"] = false
	db := clickhouse.OpenDB(chOptions)
	conn, err := clickhouse.Open(chOptions)
	if err != nil {
		logrus.Fatalf("Failed to open ClickHouse connection: %v", err)
	}

	if err != nil {
		logrus.Fatalf("Failed to create table columns cache: %v", err)
	}

	return &clickhouseDriver{
		db:                db,
		conn:              conn,
		database:          database,
		queryMapper:       NewClickHouseQueryMapper(opts...),
		fieldTypeMapper:   NewFieldTypeMapper(),
		queryTimeout:      30 * time.Second,
		typeComparer:      warehouse.NewTypeComparer(),
		tableColumnsCache: util.NewTTLCache[[]*arrow.Field](1 * time.Minute),
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

// columns retrieves all columns from the specified table as Arrow fields
func (d *clickhouseDriver) columns(ctx context.Context, table string) ([]*arrow.Field, error) {
	ctx, cancel := context.WithTimeout(ctx, d.queryTimeout)
	defer cancel()

	query := `
		SELECT name, type 
		FROM system.columns 
		WHERE database = ? AND table = ?
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

	var fields []*arrow.Field
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
		fields = append(fields, arrowField)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// If no rows were returned, the table doesn't exist
	if !hasRows {
		return nil, warehouse.NewTableNotFoundError(fmt.Sprintf("%s.%s", d.database, table))
	}

	return fields, nil
}

func (d *clickhouseDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	fields, err := d.columns(context.Background(), table)
	if err != nil {
		return nil, err
	}

	// Convert fields slice to map for comparison
	existingFields := make(map[string]*arrow.Field)
	for _, field := range fields {
		existingFields[field.Name] = field
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

	schemaFields, err := d.sortSchemaFieldsForWriting(ctx, table, schema.Fields())
	if err != nil {
		return fmt.Errorf("error sorting schema fields: %w", err)
	}

	// Construct full table name with database
	fullTableName := fmt.Sprintf("%s.%s", d.database, table)

	// Get column names and types from schema
	columns := make([]string, len(schemaFields))
	columnTypes := make([]SpecificClickhouseType, len(schemaFields))
	for i, field := range schemaFields {
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

func (d *clickhouseDriver) sortSchemaFieldsForWriting(
	ctx context.Context, table string, schemaFields []arrow.Field,
) ([]*arrow.Field, error) {
	var realFields []*arrow.Field
	var err error
	realFields, ok := d.tableColumnsCache.Get(table)
	if !ok {
		realFields, err = d.columns(ctx, table)
		if err != nil {
			return nil, fmt.Errorf("error getting table columns while sorting: %w", err)
		}
		d.tableColumnsCache.Set(table, realFields)
	}
	// at this point we have the real fields in the correct order, ready for writing
	// all that is left is do a really quick check to see if both schemas seem to be
	// compatible - other parts of the code do the deeper checks, here we only check column names
	if len(realFields) != len(schemaFields) {
		return nil, fmt.Errorf(
			"column count mismatch: realFields has %d columns, schemaFields has %d columns",
			len(realFields), len(schemaFields),
		)
	}

	realFieldNames := make(map[string]bool, len(realFields))
	for _, field := range realFields {
		realFieldNames[field.Name] = true
	}

	for _, field := range schemaFields {
		if !realFieldNames[field.Name] {
			return nil, fmt.Errorf("column %s not found in realFields", field.Name)
		}
	}

	return realFields, nil
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
