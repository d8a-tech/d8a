// Package testutils provides test utilities for the warehouse package.
package testutils

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchema is a test schema for testing the query mapper.
func TestSchema() *arrow.Schema {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "user_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
		{Name: "event_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "session_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "items", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "rating", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "metrics", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64), Nullable: true},
		{Name: "properties", Type: arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
		)), Nullable: true},
		{Name: "created_date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	}
	return arrow.NewSchema(fields, nil)
}

// SupportedArrowTypesTestCase represents a test case for supported Arrow types in the query mapper.
type SupportedArrowTypesTestCase struct {
	Name          string
	Mapper        warehouse.QueryMapper
	SupportedType arrow.DataType
}

// TestSupportedArrowTypes tests that the query mapper correctly handles all supported Arrow types.
func TestSupportedArrowTypes(t *testing.T, qm warehouse.QueryMapper) { //nolint:funlen // It's a test function
	testCases := []SupportedArrowTypesTestCase{
		{
			Name:          "String type",
			Mapper:        qm,
			SupportedType: arrow.BinaryTypes.String,
		},
		{
			Name:          "Int64 type",
			Mapper:        qm,
			SupportedType: arrow.PrimitiveTypes.Int64,
		},
		{
			Name:          "Int32 type",
			Mapper:        qm,
			SupportedType: arrow.PrimitiveTypes.Int32,
		},
		{
			Name:          "Timestamp type",
			Mapper:        qm,
			SupportedType: arrow.FixedWidthTypes.Timestamp_s,
		},
		{
			Name:          "Boolean type",
			Mapper:        qm,
			SupportedType: arrow.FixedWidthTypes.Boolean,
		},
		{
			Name:          "Float64 type",
			Mapper:        qm,
			SupportedType: arrow.PrimitiveTypes.Float64,
		},
		{
			Name:          "Float32 type",
			Mapper:        qm,
			SupportedType: arrow.PrimitiveTypes.Float32,
		},
		{
			Name:          "Date32 type",
			Mapper:        qm,
			SupportedType: arrow.FixedWidthTypes.Date32,
		},
		{
			Name:          "List of String type",
			Mapper:        qm,
			SupportedType: arrow.ListOf(arrow.BinaryTypes.String),
		},
		{
			Name:          "List of Float64 type",
			Mapper:        qm,
			SupportedType: arrow.ListOf(arrow.PrimitiveTypes.Float64),
		},
		{
			Name:          "List of Date32 type",
			Mapper:        qm,
			SupportedType: arrow.ListOf(arrow.FixedWidthTypes.Date32),
		},
		{
			Name:   "List of Struct type with primitive fields only",
			Mapper: qm,
			SupportedType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
			)),
		},
		{
			Name:   "List of Struct type with Date32 field",
			Mapper: qm,
			SupportedType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
			)),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// given
			field := arrow.Field{Name: "test_field", Type: tc.SupportedType, Nullable: false}

			// when
			result, err := tc.Mapper.Field(&field)

			// then
			require.NoError(t, err)
			assert.NotEmpty(t, result)
		})
	}
}

// UnsupportedArrowTypeErrorTestCase represents a test case for unsupported Arrow types in the query mapper.
type UnsupportedArrowTypeErrorTestCase struct {
	Name        string
	Mapper      warehouse.QueryMapper
	Unsupported arrow.DataType
}

// TestQueryMapperTypeErrors tests that the query mapper returns an error when an unsupported arrow type is used.
func TestQueryMapperTypeErrors(t *testing.T, qm warehouse.QueryMapper) { //nolint:funlen // It's a test function
	testCases := []UnsupportedArrowTypeErrorTestCase{
		{
			Name:        "Date64",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Date64,
		},
		{
			Name:        "Uint64",
			Mapper:      qm,
			Unsupported: arrow.PrimitiveTypes.Uint64,
		},
		{
			Name:        "Uint32",
			Mapper:      qm,
			Unsupported: arrow.PrimitiveTypes.Uint32,
		},
		{
			Name:        "Uint16",
			Mapper:      qm,
			Unsupported: arrow.PrimitiveTypes.Uint16,
		},
		{
			Name:        "Uint8",
			Mapper:      qm,
			Unsupported: arrow.PrimitiveTypes.Uint8,
		},
		{
			Name:        "Int16",
			Mapper:      qm,
			Unsupported: arrow.PrimitiveTypes.Int16,
		},
		{
			Name:        "Int8",
			Mapper:      qm,
			Unsupported: arrow.PrimitiveTypes.Int8,
		},
		{
			Name:        "Float16",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Float16,
		},
		{
			Name:        "Time32_s",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Time32s,
		},
		{
			Name:        "Time32_ms",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Time32ms,
		},
		{
			Name:        "Time64_us",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Time64us,
		},
		{
			Name:        "Time64_ns",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Time64ns,
		},
		{
			Name:        "Duration_s",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Duration_s,
		},
		{
			Name:        "Duration_ms",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Duration_ms,
		},
		{
			Name:        "Duration_us",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Duration_us,
		},
		{
			Name:        "Duration_ns",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.Duration_ns,
		},
		{
			Name:        "Interval_months",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.MonthInterval,
		},
		{
			Name:        "Interval_day_time",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.DayTimeInterval,
		},
		{
			Name:        "Interval_month_day_nano",
			Mapper:      qm,
			Unsupported: arrow.FixedWidthTypes.MonthDayNanoInterval,
		},
		{
			Name:        "Decimal128",
			Mapper:      qm,
			Unsupported: &arrow.Decimal128Type{Precision: 10, Scale: 2},
		},
		{
			Name:        "Decimal256",
			Mapper:      qm,
			Unsupported: &arrow.Decimal256Type{Precision: 10, Scale: 2},
		},
		{
			Name:        "Binary",
			Mapper:      qm,
			Unsupported: arrow.BinaryTypes.Binary,
		},
		{
			Name:        "LargeBinary",
			Mapper:      qm,
			Unsupported: arrow.BinaryTypes.LargeBinary,
		},
		{
			Name:        "LargeString",
			Mapper:      qm,
			Unsupported: arrow.BinaryTypes.LargeString,
		},
		{
			Name:        "FixedSizeBinary",
			Mapper:      qm,
			Unsupported: &arrow.FixedSizeBinaryType{ByteWidth: 16},
		},
		{
			Name:        "Map",
			Mapper:      qm,
			Unsupported: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
		},
		{
			Name:        "FixedSizeList",
			Mapper:      qm,
			Unsupported: arrow.FixedSizeListOf(5, arrow.PrimitiveTypes.Int32),
		},
		{
			Name:        "LargeList",
			Mapper:      qm,
			Unsupported: arrow.LargeListOf(arrow.BinaryTypes.String),
		},
		{
			Name:   "Union_sparse",
			Mapper: qm,
			Unsupported: arrow.SparseUnionOf([]arrow.Field{
				{Name: "int", Type: arrow.PrimitiveTypes.Int32},
				{Name: "string", Type: arrow.BinaryTypes.String},
			}, []arrow.UnionTypeCode{0, 1}),
		},
		{
			Name:   "Union_dense",
			Mapper: qm,
			Unsupported: arrow.DenseUnionOf([]arrow.Field{
				{Name: "int", Type: arrow.PrimitiveTypes.Int32},
				{Name: "string", Type: arrow.BinaryTypes.String},
			}, []arrow.UnionTypeCode{0, 1}),
		},
		{
			Name:   "Dictionary",
			Mapper: qm,
			Unsupported: &arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int8,
				ValueType: arrow.BinaryTypes.String,
			},
		},
		{
			Name:        "Null",
			Mapper:      qm,
			Unsupported: arrow.Null,
		},
		// Non-repeated nested fields (not supported due to ClickHouse limitations)
		{
			Name:   "Struct type (non-repeated nested)",
			Mapper: qm,
			Unsupported: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			),
		},
		{
			Name:   "Struct with device info fields",
			Mapper: qm,
			Unsupported: arrow.StructOf(
				arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "os", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "version", Type: arrow.BinaryTypes.String, Nullable: true},
			),
		},
		// Multi-level nesting (exceeds 1-level limit)
		{
			Name:   "List of Struct with nested List (multi-level nesting)",
			Mapper: qm,
			Unsupported: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "preferences", Type: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
				)), Nullable: true},
			)),
		},
		{
			Name:   "List of Struct with nested Struct (multi-level nesting)",
			Mapper: qm,
			Unsupported: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "user", Type: arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
				), Nullable: true},
				arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			)),
		},
		// Complex supported types containing unsupported fields
		{
			Name:        "List of Date64",
			Mapper:      qm,
			Unsupported: arrow.ListOf(arrow.FixedWidthTypes.Date64),
		},
		{
			Name:        "List of Uint64",
			Mapper:      qm,
			Unsupported: arrow.ListOf(arrow.PrimitiveTypes.Uint64),
		},
		{
			Name:        "List of Binary",
			Mapper:      qm,
			Unsupported: arrow.ListOf(arrow.BinaryTypes.Binary),
		},
		{
			Name:        "List of Decimal128",
			Mapper:      qm,
			Unsupported: arrow.ListOf(&arrow.Decimal128Type{Precision: 10, Scale: 2}),
		},
		{
			Name:   "Struct with Uint64 field",
			Mapper: qm,
			Unsupported: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
				arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			),
		},
		{
			Name:   "Struct with Binary field",
			Mapper: qm,
			Unsupported: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "data", Type: arrow.BinaryTypes.Binary, Nullable: true},
			),
		},
		{
			Name:   "Struct with Time64 field",
			Mapper: qm,
			Unsupported: arrow.StructOf(
				arrow.Field{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
				arrow.Field{Name: "time", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
			),
		},
		{
			Name:   "List of Struct with Decimal field",
			Mapper: qm,
			Unsupported: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 4}, Nullable: true},
			)),
		},
		{
			Name:   "Struct with nested List of unsupported type",
			Mapper: qm,
			Unsupported: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
				arrow.Field{Name: "dates", Type: arrow.ListOf(arrow.FixedWidthTypes.Date64), Nullable: true},
			),
		},
		{
			Name:   "Struct with multiple unsupported fields",
			Mapper: qm,
			Unsupported: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "user_id", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
				arrow.Field{Name: "data", Type: arrow.BinaryTypes.Binary, Nullable: true},
			),
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s(%T)", tc.Name, tc.Mapper), func(t *testing.T) {
			// given
			field := arrow.Field{Name: "test_field", Type: tc.Unsupported, Nullable: false}

			// when
			_, err := tc.Mapper.Field(&field)

			// then
			require.Error(t, err)

			var unsupportedErr *warehouse.ErrUnsupportedMapping
			assert.True(t, errors.As(err, &unsupportedErr), "error should be of type ErrUnsupportedArrowType")

			if unsupportedErr != nil {
				assert.Equal(t, tc.Unsupported, unsupportedErr.Type)
				assert.Contains(t, err.Error(), "does not support")
			}
		})
	}
}

// assertFieldsEqual recursively compares two Arrow fields including nested structures.
func assertFieldsEqual(t *testing.T, expected, actual *arrow.Field, fieldPath string) {
	t.Helper()

	// Compare field names
	assert.Equal(t, expected.Name, actual.Name,
		"%s: field names differ - expected: %s, actual: %s",
		fieldPath, expected.Name, actual.Name)

	// Compare nullability
	assert.Equal(t, expected.Nullable, actual.Nullable,
		"%s: nullability differs - expected: %t, actual: %t",
		fieldPath, expected.Nullable, actual.Nullable)

	// Compare field types
	assertTypesEqual(t, expected.Type, actual.Type, fmt.Sprintf("%s.%s", fieldPath, expected.Name))
}

// assertTypesEqual recursively compares Arrow data types, handling complex nested types.
func assertTypesEqual(t *testing.T, expected, actual arrow.DataType, typePath string) {
	t.Helper()

	result := warehouse.CompareArrowTypes(expected, actual, typePath)
	if !result.Equal {
		assert.Fail(t, result.ErrorMessage)
	}
}

// TestMissingColumns performs comprehensive tests for the MissingColumns method.
// It tests various scenarios including empty tables, partial matches, type compatibility,
// and error conditions.
func TestMissingColumns( //nolint:funlen // It's a test function
	t *testing.T,
	driver warehouse.Driver,
	tableName string,
) {
	t.Helper()

	// given
	require.NotNil(t, driver, "driver should not be nil")
	require.NotEmpty(t, tableName, "table name should not be empty")

	testSchema := TestSchema()

	// Test 1: Empty table (all columns missing)
	t.Run("empty_table_all_missing", func(t *testing.T) {
		// Create table with minimal schema (single dummy column)
		// BigQuery doesn't support completely empty tables
		dummyField := arrow.Field{Name: "dummy_column", Type: arrow.BinaryTypes.String, Nullable: true}
		minimalSchema := arrow.NewSchema([]arrow.Field{dummyField}, nil)
		createErr := driver.CreateTable(tableName+"_empty", minimalSchema)
		require.NoError(t, createErr, "minimal table creation should succeed")

		// When - check missing columns
		missing, err := driver.MissingColumns(tableName+"_empty", testSchema)

		// Then - all test schema columns should be missing
		require.NoError(t, err, "MissingColumns should succeed for minimal table")
		require.Len(t, missing, len(testSchema.Fields()), "all test schema columns should be missing")

		// Verify all fields are present in missing columns
		expectedFields := make(map[string]*arrow.Field)
		for _, field := range testSchema.Fields() {
			expectedFields[field.Name] = &field
		}

		for _, missingField := range missing {
			expectedField, exists := expectedFields[missingField.Name]
			require.True(t, exists, "missing field %s should be in expected fields", missingField.Name)
			assertFieldsEqual(t, expectedField, missingField, missingField.Name)
		}
	})

	// Test 2: Table with some matching columns
	t.Run("partial_match", func(t *testing.T) {
		// Create table with partial schema (only first 2 fields)
		partialFields := testSchema.Fields()[:2]
		partialSchema := arrow.NewSchema(partialFields, nil)
		createErr := driver.CreateTable(tableName+"_partial", partialSchema)
		require.NoError(t, createErr, "partial table creation should succeed")

		// When - check missing columns
		missing, err := driver.MissingColumns(tableName+"_partial", testSchema)

		// Then - only missing columns should be returned
		require.NoError(t, err, "MissingColumns should succeed for partial table")
		expectedMissingCount := len(testSchema.Fields()) - len(partialFields)
		require.Len(t, missing, expectedMissingCount, "should return only missing columns")

		// Verify returned fields are the ones not in partial schema
		for _, missingField := range missing {
			found := false
			for _, existingField := range partialFields {
				if existingField.Name == missingField.Name {
					found = true
					break
				}
			}
			require.False(t, found, "missing field %s should not exist in partial schema", missingField.Name)
		}
	})

	// Test 3: Non-existent table
	t.Run("non_existent_table", func(t *testing.T) {
		// When - check missing columns for non-existent table
		nonExistentTable := "non_existent_table_xyz"
		_, err := driver.MissingColumns(nonExistentTable, testSchema)

		// Then - should return table not found error
		require.Error(t, err, "MissingColumns should fail for non-existent table")
		var tableNotFoundErr *warehouse.ErrTableNotFound
		require.ErrorAs(t, err, &tableNotFoundErr, "should return ErrTableNotFound")
	})

	// Test 4: Incompatible column types
	t.Run("incompatible_column_types", func(t *testing.T) {
		// Create table with a column that has the same name but different type
		incompatibleFields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},      // Different type than test schema
			{Name: "user_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false}, // Same type as test schema
		}
		incompatibleSchema := arrow.NewSchema(incompatibleFields, nil)
		createErr := driver.CreateTable(tableName+"_incompatible", incompatibleSchema)
		require.NoError(t, createErr, "incompatible table creation should succeed")

		// When - check missing columns
		_, err := driver.MissingColumns(tableName+"_incompatible", testSchema)

		// Then - should return all columns including the one with incompatible type
		require.Error(t, err, "MissingColumns should fail for table with incompatible types")
		var multiTypeErr *warehouse.ErrMultipleTypeIncompatible
		require.True(t, errors.As(err, &multiTypeErr), "error should be of type ErrMultipleTypeIncompatible")
		require.Len(t, multiTypeErr.Errors, 1, "should have exactly one type error")
		assert.Equal(t, "id", multiTypeErr.Errors[0].ColumnName,
			"should return the column name of the incompatible type")
	})
}

func events() []*schema.Event {
	hits := []*hits.Hit{
		hits.New(),
		hits.New(),
	}

	events := make([]*schema.Event, 0, len(hits))
	for _, hit := range hits {
		events = append(events, &schema.Event{
			Metadata: map[string]any{},
			Values:   map[string]any{},
			BoundHit: hit,
		})
	}
	return events
}

// generateSampleTestData creates a map with sample test data for all test schema fields.
func generateSampleTestData() map[string]any {
	return map[string]any{
		"id":         "evt_12345",
		"user_id":    int64(42),
		"timestamp":  time.Now(),
		"event_type": "page_view",
		"session_id": "sess_abcdef",
		"is_active":  true,
		"score":      85.5,
		"count":      int64(10),
		"items":      int32(5),
		"rating":     float32(4.2),
		"tags":       []any{"web", "mobile", "analytics"},
		"metrics":    []any{1.5, 2.3, 4.7},
		"properties": []any{
			map[string]any{
				"key":   "campaign",
				"value": "test_campaign",
			},
			map[string]any{
				"key":   "source",
				"value": "organic",
			},
		},
		"created_date": time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
	}
}

// TestBasicWrites performs a simple test for the Write command.
// It creates a table first, then writes data to it, ensuring both operations succeed.
func TestBasicWrites(t *testing.T, driver warehouse.Driver, tableName string) {
	t.Helper()

	// given
	require.NotNil(t, driver, "driver should not be nil")
	require.NotEmpty(t, tableName, "table name should not be empty")

	testSchema := TestSchema()
	testEvents := events()

	rows := make([]map[string]any, len(testEvents))
	for i := range testEvents {
		rows[i] = generateSampleTestData()
	}

	// when - create table
	createErr := driver.CreateTable(tableName, testSchema)

	// then - table creation should succeed
	require.NoError(t, createErr, "CreateTable should succeed")

	// when - write data to table
	writeErr := driver.Write(context.Background(), tableName, testSchema, rows)

	// then - write should succeed
	require.NoError(t, writeErr, "Write should succeed")
}

// TestAddColumn performs a simple test for the AddColumn command.
func TestAddColumn(t *testing.T, driver warehouse.Driver, tableName string) { //nolint:funlen // It's a test function
	t.Helper()

	// given
	require.NotNil(t, driver, "driver should not be nil")
	require.NotEmpty(t, tableName, "table name should not be empty")

	testSchema := TestSchema()

	// when - create table
	createErr := driver.CreateTable(tableName, testSchema)

	// then - table creation should succeed
	require.NoError(t, createErr, "CreateTable should succeed")

	newColumn := &arrow.Field{Name: "new_column", Type: arrow.BinaryTypes.String, Nullable: true}

	t.Run("add_column_should_succeed", func(t *testing.T) {
		// when
		addColumnErr := driver.AddColumn(
			tableName,
			newColumn,
		)

		// then - add column should succeed
		require.NoError(t, addColumnErr, "AddColumn should succeed")
	})

	t.Run("add_column_should_fail_if_it_already_exists", func(t *testing.T) {
		addColumnErr := driver.AddColumn(
			tableName,
			newColumn,
		)

		// then
		require.Error(t, addColumnErr, "AddColumn should fail if column already exists")
		var columnAlreadyExistsErr *warehouse.ErrColumnAlreadyExists
		require.ErrorAs(t, addColumnErr, &columnAlreadyExistsErr, "error should be of type ErrColumnAlreadyExists")
	})

	t.Run("write_with_new_column_should_succeed", func(t *testing.T) {
		// given
		testEvents := events()
		rows := make([]map[string]any, len(testEvents))
		for i := range testEvents {
			row := generateSampleTestData()
			row["new_column"] = "test"
			rows[i] = row
		}
		testSchemaWithNew := arrow.NewSchema(append(testSchema.Fields(), *newColumn), nil)

		// when
		writeErr := driver.Write(context.Background(), tableName, testSchemaWithNew, rows)

		// then
		require.NoError(t, writeErr, "Write should succeed")
	})
}

// TestCreateTable performs tests for the CreateTable command.
// It tests that creating a table twice results in an error.
func TestCreateTable(t *testing.T, driver warehouse.Driver, tableName string) {
	t.Helper()

	// given
	require.NotNil(t, driver, "driver should not be nil")
	require.NotEmpty(t, tableName, "table name should not be empty")

	testSchema := TestSchema()

	t.Run("create_table_should_succeed", func(t *testing.T) {
		// when
		createErr := driver.CreateTable(tableName+"_create_test", testSchema)

		// then
		require.NoError(t, createErr, "CreateTable should succeed on first attempt")
	})

	t.Run("missing_columns_should_yield_empty_list", func(t *testing.T) {
		// when
		missing, err := driver.MissingColumns(tableName+"_create_test", testSchema)

		// then
		require.NoError(t, err, "MissingColumns should succeed")
		require.Empty(t, missing, "missing columns should be empty")
	})

	t.Run("create_table_twice_should_fail", func(t *testing.T) {
		// given - table already exists from previous test or create it first
		firstCreateErr := driver.CreateTable(tableName+"_duplicate_test", testSchema)
		require.NoError(t, firstCreateErr, "First CreateTable should succeed")

		// when - try to create the same table again
		secondCreateErr := driver.CreateTable(tableName+"_duplicate_test", testSchema)

		// then - should return table already exists error
		require.Error(t, secondCreateErr, "CreateTable should fail when table already exists")
		var tableAlreadyExistsErr *warehouse.ErrTableAlreadyExists
		require.ErrorAs(t, secondCreateErr, &tableAlreadyExistsErr, "error should be of type ErrTableAlreadyExists")
	})
}

// TestComplexWrites performs tests for the Write command with complex data types.
func TestComplexWrites(t *testing.T, driver warehouse.Driver, tableName string) { // nolint:funlen,lll // It's a test function
	t.Helper()

	// given
	require.NotNil(t, driver, "driver should not be nil")
	require.NotEmpty(t, tableName, "table name should not be empty")

	testSchema := TestSchema()

	t.Run("create_table_should_succeed", func(t *testing.T) {
		// when
		createErr := driver.CreateTable(tableName+"_create_test", testSchema)

		// then
		require.NoError(t, createErr, "CreateTable should succeed on first attempt")
	})

	validRow := map[string]any{
		"id":           "123",
		"user_id":      int64(42),
		"timestamp":    nil,
		"event_type":   "page_view",
		"session_id":   "sess_abcdef",
		"is_active":    true,
		"score":        85.5,
		"count":        int64(99),
		"items":        int32(3),
		"rating":       float32(4.2),
		"tags":         nil,
		"metrics":      nil,
		"properties":   nil,
		"created_date": nil,
	}

	cases := []struct {
		name    string
		success bool
		rows    []map[string]any
	}{
		{
			name:    "empty_rows",
			success: true,
			rows:    []map[string]any{},
		},
		{
			name:    "valid_row_with_null_values",
			success: false,
			rows:    []map[string]any{validRow},
		},
		{
			name:    "valid_row_with_collections_mixed_null_values",
			success: true,
			rows: []map[string]any{func() map[string]any {
				theCopy := maps.Clone(validRow)
				theCopy["tags"] = []any{"mobile", "analytics"}
				theCopy["metrics"] = []any{2.3, 4.7}
				theCopy["properties"] = []any{
					map[string]any{
						"key":   "campaign",
						"value": nil,
					},
					map[string]any{
						"key":   nil,
						"value": "organic",
					},
				}
				return theCopy
			}()},
		},
		{
			name:    "missing_single_non_nullable_column",
			success: false,
			rows: []map[string]any{func() map[string]any {
				theCopy := maps.Clone(validRow)
				delete(theCopy, "id")
				return theCopy
			}()},
		},
		{
			name:    "missing_single_nullable_column",
			success: false, // It should still fail, even if the column is null you need to pass it
			rows: []map[string]any{func() map[string]any {
				theCopy := maps.Clone(validRow)
				delete(theCopy, "properties")
				return theCopy
			}()},
		},
		{
			name:    "invalid_column_type_for_score_field",
			success: false,
			rows: []map[string]any{func() map[string]any {
				theCopy := maps.Clone(validRow)
				theCopy["score"] = "not_a_number"
				return theCopy
			}()},
		},
		{
			name:    "invalid_column_type_for_event_type_field",
			success: false,
			rows: []map[string]any{func() map[string]any {
				theCopy := maps.Clone(validRow)
				theCopy["event_type"] = 42
				return theCopy
			}()},
		},
		{
			name:    "null_in_non_nullable_field",
			success: false,
			rows: []map[string]any{func() map[string]any {
				theCopy := maps.Clone(validRow)
				theCopy["id"] = nil
				return theCopy
			}()},
		},
		{
			name:    "single_empty_row",
			success: false,
			rows:    []map[string]any{{}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			writeErr := driver.Write(context.Background(), tableName+"_create_test", testSchema, tc.rows)

			// then
			if tc.success {
				require.NoError(t, writeErr, "Write should succeed")
			} else {
				require.Error(t, writeErr, "Write should fail")
			}
		})
	}
}
