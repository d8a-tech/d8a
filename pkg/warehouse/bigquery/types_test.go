package bigquery

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
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
	arrowType                    warehouse.ArrowType
	expectedBQType               SpecificBigQueryType
	expectedConvertBackArrowType *warehouse.ArrowType
	customMapper                 warehouse.FieldTypeMapper[SpecificBigQueryType] // for special cases
	useNestedMapper              bool                                            // for testing nested mapper directly
	useNullableMapper            bool                                            // for testing nullable mapper directly
	useArrayMapper               bool                                            // for testing array mapper directly
	// skipWarehouseToArrow bool                                            // skip warehouse to arrow test
}

func getTestCases() []TypeMappingTestCase {
	return []TypeMappingTestCase{
		{
			BaseTestCase: BaseTestCase{
				name:        "string primitive",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.BinaryTypes.String},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.StringFieldType,
				Required:  true,
				Repeated:  false,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "int64 primitive",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int64},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.IntegerFieldType,
				Required:  true,
				Repeated:  false,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "float64 primitive",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float64},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.FloatFieldType,
				Required:  true,
				Repeated:  false,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "bool primitive",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Boolean},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.BooleanFieldType,
				Required:  true,
				Repeated:  false,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "int32 primitive",
				expectError: false,
			},
			arrowType:                    warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int32},
			expectedConvertBackArrowType: &warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int64},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.IntegerFieldType,
				Required:  true,
				Repeated:  false,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "float32 primitive",
				expectError: false,
			},
			arrowType:                    warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float32},
			expectedConvertBackArrowType: &warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float64},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.FloatFieldType,
				Required:  true,
				Repeated:  false,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "timestamp primitive",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Timestamp_s},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.TimestampFieldType,
				Required:  true,
				Repeated:  false,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "date32 primitive",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Date32},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.DateFieldType,
				Required:  true,
				Repeated:  false,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of strings",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.BinaryTypes.String)},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.StringFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of int64",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.IntegerFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of float64",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Float64)},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.FloatFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of bools",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.FixedWidthTypes.Boolean)},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.BooleanFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of int32",
				expectError: false,
			},
			arrowType:                    warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int32)},
			expectedConvertBackArrowType: &warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.IntegerFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of float32",
				expectError: false,
			},
			arrowType:                    warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
			expectedConvertBackArrowType: &warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Float64)},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.FloatFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of timestamps",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.FixedWidthTypes.Timestamp_s)},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.TimestampFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "array of date32",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.FixedWidthTypes.Date32)},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.DateFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with string fields",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
				)),
			},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.RecordFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with mixed primitive fields",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				)),
			},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.RecordFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "list of struct with all primitive types",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.ListOf(arrow.StructOf(
					arrow.Field{Name: "str_field", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "int64_field", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
					arrow.Field{Name: "int32_field", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
					arrow.Field{Name: "float64_field", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					arrow.Field{Name: "float32_field", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
					arrow.Field{Name: "bool_field", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
					arrow.Field{Name: "timestamp_field", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
					arrow.Field{Name: "date_field", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
				)),
			},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.RecordFieldType,
				Required:  false,
				Repeated:  true,
			},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nested struct with string fields",
				expectError: true,
				errorType: warehouse.NewUnsupportedMappingErr(arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "description", Type: arrow.BinaryTypes.String, Nullable: true},
				), MapperName),
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "description", Type: arrow.BinaryTypes.String, Nullable: true},
				),
			},
			// Remove expected BQ type since this should fail
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nested struct with mixed types",
				expectError: true,
				errorType: warehouse.NewUnsupportedMappingErr(arrow.StructOf(
					arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					arrow.Field{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				), MapperName),
			},
			arrowType: warehouse.ArrowType{
				ArrowDataType: arrow.StructOf(
					arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					arrow.Field{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				),
			},
			// Remove expected BQ type since this should fail
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nullable string",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.BinaryTypes.String, Nullable: true},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.StringFieldType,
				Required:  false,
				Repeated:  false,
			},
			useNullableMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nullable int64",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int64, Nullable: true},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.IntegerFieldType,
				Required:  false,
				Repeated:  false,
			},
			useNullableMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nullable float64",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float64, Nullable: true},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.FloatFieldType,
				Required:  false,
				Repeated:  false,
			},
			useNullableMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nullable bool",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Boolean, Nullable: true},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.BooleanFieldType,
				Required:  false,
				Repeated:  false,
			},
			useNullableMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nullable int32",
				expectError: false,
			},
			arrowType:                    warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int32, Nullable: true},
			expectedConvertBackArrowType: &warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int64},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.IntegerFieldType,
				Required:  false,
				Repeated:  false,
			},
			useNullableMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nullable float32",
				expectError: false,
			},
			arrowType:                    warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float32, Nullable: true},
			expectedConvertBackArrowType: &warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float64},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.FloatFieldType,
				Required:  false,
				Repeated:  false,
			},
			useNullableMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nullable timestamp",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.TimestampFieldType,
				Required:  false,
				Repeated:  false,
			},
			useNullableMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "nullable date32",
				expectError: false,
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Date32, Nullable: true},
			expectedBQType: SpecificBigQueryType{
				FieldType: bigquery.DateFieldType,
				Required:  false,
				Repeated:  false,
			},
			useNullableMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "unsupported type",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.PrimitiveTypes.Int8, MapperName),
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int8},
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "unsupported element type in array",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.PrimitiveTypes.Int8, MapperName),
			},
			arrowType: warehouse.ArrowType{ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int8)},
			customMapper: warehouse.NewTypeMapper([]warehouse.FieldTypeMapper[SpecificBigQueryType]{
				&bigQueryStringTypeMapper{},
			}),
			useArrayMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "non-struct type to nested mapper",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.BinaryTypes.String, MapperName),
			},
			arrowType:       warehouse.ArrowType{ArrowDataType: arrow.BinaryTypes.String},
			useNestedMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "non-nullable type to nullable mapper",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.BinaryTypes.String, MapperName),
			},
			arrowType:         warehouse.ArrowType{ArrowDataType: arrow.BinaryTypes.String, Nullable: false},
			useNullableMapper: true,
		},
		{
			BaseTestCase: BaseTestCase{
				name:        "non-list type to array mapper",
				expectError: true,
				errorType:   warehouse.NewUnsupportedMappingErr(arrow.BinaryTypes.String, MapperName),
			},
			arrowType:      warehouse.ArrowType{ArrowDataType: arrow.BinaryTypes.String},
			useArrayMapper: true,
		},
	}
}

func TestArrowToWarehouse(t *testing.T) {
	testCases := getTestCases()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			var mapper warehouse.FieldTypeMapper[SpecificBigQueryType]

			switch {
			case tc.customMapper != nil:
				if tc.useArrayMapper {
					mapper = &bigQueryArrayTypeMapper{SubMapper: tc.customMapper}
				} else {
					mapper = tc.customMapper
				}
			case tc.useNestedMapper:
				mapper = &bigQueryNestedTypeMapper{SubMapper: NewFieldTypeMapper()}
			case tc.useNullableMapper:
				mapper = &bigQueryNullableTypeMapper{SubMapper: NewFieldTypeMapper()}
			case tc.useArrayMapper:
				mapper = &bigQueryArrayTypeMapper{SubMapper: NewFieldTypeMapper()}
			default:
				mapper = NewFieldTypeMapper()
			}

			// when
			bqType, err := mapper.ArrowToWarehouse(tc.arrowType)

			// then
			if tc.expectError {
				require.Error(t, err)
				if tc.errorType != nil {
					assert.IsType(t, tc.errorType, err)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedBQType.FieldType, bqType.FieldType)
				assert.Equal(t, tc.expectedBQType.Required, bqType.Required)
				assert.Equal(t, tc.expectedBQType.Repeated, bqType.Repeated)
				assert.NotNil(t, bqType.FormatFunc)
			}
		})
	}
}

func TestWarehouseToArrow(t *testing.T) {
	testCases := getTestCases()

	for _, tc := range testCases {
		if tc.expectError {
			continue // Skip error cases and cases that don't support warehouse to arrow
		}

		t.Run(tc.name, func(t *testing.T) {
			// given
			var mapper warehouse.FieldTypeMapper[SpecificBigQueryType]

			if tc.useNullableMapper {
				mapper = &bigQueryNullableTypeMapper{SubMapper: NewFieldTypeMapper()}
			} else {
				mapper = NewFieldTypeMapper()
			}

			// First create the BQ type from Arrow type
			bqType, err := mapper.ArrowToWarehouse(tc.arrowType)
			require.NoError(t, err)

			// when
			backToArrow, err := mapper.WarehouseToArrow(bqType)

			// then
			require.NoError(t, err)
			arrowType := tc.arrowType
			if tc.expectedConvertBackArrowType != nil {
				arrowType = *tc.expectedConvertBackArrowType
			}
			assert.Equal(t, arrowType.ArrowDataType.ID(), backToArrow.ArrowDataType.ID())
		})
	}
}

func TestRoundTripMapping(t *testing.T) {
	testCases := getTestCases()

	for _, tc := range testCases {
		if tc.expectError {
			continue // Skip error cases and unsupported round-trip cases
		}

		t.Run(tc.name, func(t *testing.T) {
			// given
			var mapper warehouse.FieldTypeMapper[SpecificBigQueryType]

			if tc.useNullableMapper {
				mapper = &bigQueryNullableTypeMapper{SubMapper: NewFieldTypeMapper()}
			} else {
				mapper = NewFieldTypeMapper()
			}

			// when - Arrow to BigQuery
			bqType, err := mapper.ArrowToWarehouse(tc.arrowType)

			// then
			require.NoError(t, err)
			assert.Equal(t, tc.expectedBQType.FieldType, bqType.FieldType)
			assert.Equal(t, tc.expectedBQType.Required, bqType.Required)
			assert.Equal(t, tc.expectedBQType.Repeated, bqType.Repeated)

			// when - BigQuery to Arrow
			backToArrow, err := mapper.WarehouseToArrow(bqType)

			// then
			require.NoError(t, err)
			arrowType := tc.arrowType
			if tc.expectedConvertBackArrowType != nil {
				arrowType = *tc.expectedConvertBackArrowType
			}
			assert.Equal(t, arrowType.ArrowDataType.ID(), backToArrow.ArrowDataType.ID())
		})
	}
}

func TestFormatFunction(t *testing.T) {
	testCases := []struct {
		name         string
		bqType       SpecificBigQueryType
		input        any
		expected     any
		expectError  bool
		errorMessage string
	}{
		{
			name:     "string format",
			bqType:   bigQueryString,
			input:    "test",
			expected: "test",
		},
		{
			name:     "int64 format - int64 input",
			bqType:   bigQueryInt64,
			input:    int64(42),
			expected: int64(42),
		},
		{
			name:     "int64 format - int input",
			bqType:   bigQueryInt64,
			input:    42,
			expected: int64(42),
		},
		{
			name:     "int64 format - int32 input",
			bqType:   bigQueryInt64,
			input:    int32(42),
			expected: int64(42),
		},
		{
			name:         "int64 format - invalid input",
			bqType:       bigQueryInt64,
			input:        "not a number",
			expectError:  true,
			errorMessage: "expected int64-compatible type, got string",
		},
		{
			name:     "float64 format - float64 input",
			bqType:   bigQueryFloat64,
			input:    3.14,
			expected: 3.14,
		},
		{
			name:     "float64 format - float32 input",
			bqType:   bigQueryFloat64,
			input:    float32(3.14),
			expected: float64(float32(3.14)),
		},
		{
			name:         "float64 format - invalid input",
			bqType:       bigQueryFloat64,
			input:        "not a number",
			expectError:  true,
			errorMessage: "expected float64-compatible type, got string",
		},
		{
			name:     "bool format",
			bqType:   bigQueryBool,
			input:    true,
			expected: true,
		},
		{
			name:     "bool format - nil",
			bqType:   bigQueryBool,
			input:    nil,
			expected: nil,
		},
		{
			name:     "int32 format - int32 input",
			bqType:   bigQueryInt32,
			input:    int32(42),
			expected: int32(42),
		},
		{
			name:     "int32 format - int input",
			bqType:   bigQueryInt32,
			input:    42,
			expected: int32(42),
		},
		{
			name:     "int32 format - int64 input",
			bqType:   bigQueryInt32,
			input:    int64(42),
			expected: int32(42),
		},
		{
			name:         "int32 format (overflow) - int64 input",
			bqType:       bigQueryInt32,
			input:        int64(17179869184),
			expectError:  true,
			errorMessage: "int64 value 17179869184 overflows int32 range",
		},
		{
			name:         "int32 format - invalid input",
			bqType:       bigQueryInt32,
			input:        "not a number",
			expectError:  true,
			errorMessage: "expected int32-compatible type, got string",
		},
		{
			name:     "float32 format - float32 input",
			bqType:   bigQueryFloat32,
			input:    float32(3.14),
			expected: float32(3.14),
		},
		{
			name:     "float32 format - float64 input",
			bqType:   bigQueryFloat32,
			input:    3.14,
			expected: float32(3.14),
		},
		{
			name:         "float32 format (overflow) - float64 input",
			bqType:       bigQueryFloat32,
			input:        float64(1e40),
			expectError:  true,
			errorMessage: "float64 value 1e+40 overflows float32 range",
		},
		{
			name:         "float32 format - invalid input",
			bqType:       bigQueryFloat32,
			input:        "not a number",
			expectError:  true,
			errorMessage: "expected float32-compatible type, got string",
		},
		{
			name:     "timestamp format",
			bqType:   bigQueryTimestamp,
			input:    "2023-01-01T12:00:00Z",
			expected: "2023-01-01T12:00:00Z",
		},
		{
			name:     "date32 format - time.Time input",
			bqType:   bigQueryDate32,
			input:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			expected: "2024-01-15",
		},
		{
			name:     "date32 format - string input",
			bqType:   bigQueryDate32,
			input:    "2024-01-15",
			expected: "2024-01-15",
		},
		{
			name:         "date32 format - invalid input",
			bqType:       bigQueryDate32,
			input:        12345,
			expectError:  true,
			errorMessage: "expected time.Time or date string, got int",
		},
		{
			name: "collection including nil",
			bqType: func() SpecificBigQueryType {
				m := NewFieldTypeMapper()
				bt, err := m.ArrowToWarehouse(
					warehouse.ArrowType{
						ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int64),
						Nullable:      true,
					},
				)
				require.NoError(t, err)
				return bt
			}(),
			input:    []any{int64(1), nil, int64(3)},
			expected: []any{int64(1), nil, int64(3)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			metadata := arrow.NewMetadata([]string{}, []string{})

			// when
			result, err := tc.bqType.Format(tc.input, metadata)

			// then
			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMessage)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
