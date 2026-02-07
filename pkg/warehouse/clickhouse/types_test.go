package clickhouse

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/d8a-tech/d8a/pkg/warehouse/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BaseTestCase contains common test case attributes
type BaseTestCase struct {
	name        string
	expectError bool
	errorType   any
}

// TypeMappingTestCase contains all data needed for type mapping tests
type TypeMappingTestCase struct {
	BaseTestCase
	arrowType       warehouse.ArrowType
	expectedCHType  string
	customMapper    warehouse.FieldTypeMapper[SpecificClickhouseType] // for special cases
	useNestedMapper bool                                              // for testing nested mapper directly
}

func getTestCases() []TypeMappingTestCase {
	return []TypeMappingTestCase{
		{
			BaseTestCase: BaseTestCase{
				name:        "array of strings",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.BinaryTypes.String)},
			expectedCHType: "Array(String)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of int64",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			expectedCHType: "Array(Int64)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "int64 primitive",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int64},
			expectedCHType: "Int64",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "int32 primitive",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int32},
			expectedCHType: "Int32",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "float64 primitive",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float64},
			expectedCHType: "Float64",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "float32 primitive",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float32},
			expectedCHType: "Float32",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "bool primitive",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Boolean},
			expectedCHType: "Bool",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "string primitive",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.BinaryTypes.String},
			expectedCHType: "String",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of int32",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int32)},
			expectedCHType: "Array(Int32)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of float32",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
			expectedCHType: "Array(Float32)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nested struct with unsupported type",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.PrimitiveTypes.Int8, ClickhouseMapperName),
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
				),
				Nullable: true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nested struct with int64 fields",
				expectError: true,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.StructOf(
					arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				),
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nested struct with mixed primitive types",
				expectError: true,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.StructOf(
					arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
					arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
					arrow.Field{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				),
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "complex nested with arrays",
				expectError: true,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.StructOf(
					arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
					arrow.Field{Name: "scores", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64), Nullable: true},
				),
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "unsupported element type in array",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.PrimitiveTypes.Int8, ClickhouseMapperName),
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int8)},
			customMapper: warehouse.NewTypeMapper([]warehouse.FieldTypeMapper[SpecificClickhouseType]{
				&clickhouseStringTypeMapper{},
			}),
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "non-struct type to nested mapper",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.BinaryTypes.String, ClickhouseMapperName),
			},
			arrowType:       warehouse.ArrowType{ArrowDataType: arrow.BinaryTypes.String},
			useNestedMapper: true,
		},

		// === SUPPORTED REPEATED NESTED CASES ===
		// List of Struct with primitive fields only (1-level repeated nesting)
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with string and int64 fields",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				)),
			},
			expectedCHType: "Nested(key String, value Int64)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with mixed primitive types",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
					arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					arrow.Field{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				)),
			},
			expectedCHType: "Nested(id Int32, score Float64, active Bool, name String)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with timestamp field",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "event_name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: false},
				)),
			},
			expectedCHType: "Nested(event_name String, timestamp DateTime64(0))",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with all supported primitive types",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "str_field", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "int32_field", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
					arrow.Field{Name: "int64_field", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
					arrow.Field{Name: "float32_field", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
					arrow.Field{Name: "float64_field", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					arrow.Field{Name: "bool_field", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
					arrow.Field{Name: "timestamp_field", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: false},
				)),
			},
			expectedCHType: "Nested(str_field String, int32_field Int32, int64_field Int64, " +
				"float32_field Float32, float64_field Float64, bool_field Bool, " +
				"timestamp_field DateTime64(0))",
		},

		// === LOWCARDINALITY TESTS ===
		{
			BaseTestCase: BaseTestCase{
				name:        "string with low cardinality metadata",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.BinaryTypes.String,
				Metadata: arrow.NewMetadata(
					[]string{meta.ClickhouseLowCardinalityMetadata},
					[]string{"true"},
				),
			},
			expectedCHType: "LowCardinality(String)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "int64 with low cardinality metadata",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.PrimitiveTypes.Int64,
				Metadata: arrow.NewMetadata(
					[]string{meta.ClickhouseLowCardinalityMetadata},
					[]string{"true"},
				),
			},
			expectedCHType: "LowCardinality(Int64)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nullable string with low cardinality metadata",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.BinaryTypes.String,
				Nullable:      true,
				Metadata: arrow.NewMetadata(
					[]string{meta.ClickhouseLowCardinalityMetadata},
					[]string{"true"},
				),
			},
			expectedCHType: "LowCardinality(String)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "string without low cardinality metadata",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.BinaryTypes.String,
			},
			expectedCHType: "String",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "string with low cardinality metadata set to false",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.BinaryTypes.String,
				Metadata: arrow.NewMetadata(
					[]string{meta.ClickhouseLowCardinalityMetadata},
					[]string{"false"},
				),
			},
			expectedCHType: "String",
		},

		// === UNSUPPORTED REPEATED NESTED CASES ===
		// List of Struct containing unsupported primitive types
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with unsupported int8 field",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.PrimitiveTypes.Int8, ClickhouseMapperName),
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
				)),
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with unsupported uint64 field",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.PrimitiveTypes.Uint64, ClickhouseMapperName),
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
					arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
				)),
			},
		},

		// Multi-level nesting (exceeds 1-level limit) - List of Struct containing nested structures
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with nested list field (multi-level nesting)",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.ListOf(arrow.BinaryTypes.String), ClickhouseMapperName),
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
				)),
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with nested struct field (multi-level nesting)",
				expectError: true,
				errorType: warehouse.NewUnsupportedMappingErr(
					arrow.StructOf(arrow.Field{Name: "nested_name", Type: arrow.BinaryTypes.String, Nullable: true}),
					ClickhouseMapperName,
				),
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
					arrow.Field{Name: "nested_data", Type: arrow.StructOf(
						arrow.Field{Name: "nested_name", Type: arrow.BinaryTypes.String, Nullable: true},
					), Nullable: true},
				)),
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with complex multi-level nesting",
				expectError: true,
				errorType: warehouse.NewUnsupportedMappingErr(
					arrow.ListOf(arrow.StructOf(
						arrow.Field{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
						arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
					)),
					ClickhouseMapperName,
				),
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "user", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "preferences", Type: arrow.ListOf(arrow.StructOf(
						arrow.Field{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
						arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
					)), Nullable: true},
				)),
			},
		},

		// List of Struct with mixed supported and unsupported field types
		// Date32 primitive type (should be supported)
		{
			BaseTestCase: BaseTestCase{
				name:        "date32 primitive",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Date32},
			expectedCHType: "Date32",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of date32",
				expectError: false,
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.FixedWidthTypes.Date32)},
			expectedCHType: "Array(Date32)",
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with date32 field",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					arrow.Field{Name: "birth_date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
				)),
			},
			expectedCHType: "Nested(name String, score Float64, birth_date Date32)",
		},
	}
}

func TestArrowToWarehouse(t *testing.T) {
	testCases := getTestCases()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			var mapper warehouse.FieldTypeMapper[SpecificClickhouseType]

			switch {
			case tc.customMapper != nil:
				mapper = &clickhouseArrayTypeMapper{SubMapper: tc.customMapper}
			case tc.useNestedMapper:
				mapper = newClickhouseNestedTypeMapper(NewFieldTypeMapper())
			default:
				mapper = NewFieldTypeMapper()
			}

			// when
			chType, err := mapper.ArrowToWarehouse(tc.arrowType)

			// then
			if tc.expectError {
				require.Error(t, err)
				if tc.errorType != nil {
					assert.IsType(t, tc.errorType, err)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedCHType, chType.TypeAsString)
			}
		})
	}
}

func TestWarehouseToArrow(t *testing.T) {
	testCases := getTestCases()

	for _, tc := range testCases {
		if tc.expectError {
			continue // Skip error cases for WarehouseToArrow since they can't produce valid CH types
		}

		t.Run(tc.name, func(t *testing.T) {
			// given
			mapper := NewFieldTypeMapper()

			// First create the CH type from Arrow type
			chType, err := mapper.ArrowToWarehouse(tc.arrowType)
			require.NoError(t, err)

			// when
			backToArrow, err := mapper.WarehouseToArrow(chType)

			// then
			require.NoError(t, err)
			assert.Equal(t, tc.arrowType.ArrowDataType.ID(), backToArrow.ArrowDataType.ID())
		})
	}
}

func TestRoundTripMapping(t *testing.T) {
	testCases := getTestCases()

	for _, tc := range testCases {
		if tc.expectError {
			continue // Skip error cases for round-trip tests
		}

		t.Run(tc.name, func(t *testing.T) {
			// given
			mapper := NewFieldTypeMapper()

			// when - Arrow to ClickHouse
			chType, err := mapper.ArrowToWarehouse(tc.arrowType)

			// then
			require.NoError(t, err)
			assert.Equal(t, tc.expectedCHType, chType.TypeAsString)

			// when - ClickHouse to Arrow
			backToArrow, err := mapper.WarehouseToArrow(chType)

			// then
			require.NoError(t, err)
			assert.Equal(t, tc.arrowType.ArrowDataType.ID(), backToArrow.ArrowDataType.ID())
		})
	}
}

func TestLowCardinalityStripping(t *testing.T) {
	t.Run("LowCardinality(String) strips to plain String", func(t *testing.T) {
		// given
		mapper := NewFieldTypeMapper()
		chType := anyType("LowCardinality(String)")

		// when
		arrowType, err := mapper.WarehouseToArrow(chType)

		// then
		require.NoError(t, err)
		assert.Equal(t, arrow.BinaryTypes.String.ID(), arrowType.ArrowDataType.ID())
		// Verify no LowCardinality metadata is present
		_, hasMetadata := arrowType.Metadata.GetValue(meta.ClickhouseLowCardinalityMetadata)
		assert.False(t, hasMetadata, "LowCardinality metadata should not be present after stripping")
	})

	t.Run("nullable string with LowCardinality has DEFAULT modifier", func(t *testing.T) {
		// given
		mapper := NewFieldTypeMapper()
		arrowType := warehouse.ArrowType{
			ArrowDataType: arrow.BinaryTypes.String,
			Nullable:      true,
			Metadata: arrow.NewMetadata(
				[]string{meta.ClickhouseLowCardinalityMetadata},
				[]string{"true"},
			),
		}

		// when
		chType, err := mapper.ArrowToWarehouse(arrowType)

		// then
		require.NoError(t, err)
		assert.Equal(t, "LowCardinality(String)", chType.TypeAsString)
		assert.Equal(t, "DEFAULT", chType.ColumnModifiers)
		assert.Equal(t, "''", chType.DefaultSQLExpression)
	})
}
