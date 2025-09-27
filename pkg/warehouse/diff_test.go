package warehouse

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

// TypeComparisonTestCase represents a test case for comparing Arrow data types
type TypeComparisonTestCase struct {
	Name             string
	ExpectedType     arrow.DataType
	ActualType       arrow.DataType
	TypePath         string
	ExpectedEqual    bool
	ExpectedErrorMsg string
}

func TestCompareArrowTypes_PrimitiveTypes(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name:          "identical string types",
			ExpectedType:  arrow.BinaryTypes.String,
			ActualType:    arrow.BinaryTypes.String,
			TypePath:      "root.field",
			ExpectedEqual: true,
		},
		{
			Name:          "identical int64 types",
			ExpectedType:  arrow.PrimitiveTypes.Int64,
			ActualType:    arrow.PrimitiveTypes.Int64,
			TypePath:      "root.field",
			ExpectedEqual: true,
		},
		{
			Name:          "identical float64 types",
			ExpectedType:  arrow.PrimitiveTypes.Float64,
			ActualType:    arrow.PrimitiveTypes.Float64,
			TypePath:      "root.field",
			ExpectedEqual: true,
		},
		{
			Name:          "identical boolean types",
			ExpectedType:  arrow.FixedWidthTypes.Boolean,
			ActualType:    arrow.FixedWidthTypes.Boolean,
			TypePath:      "root.field",
			ExpectedEqual: true,
		},
		{
			Name:             "different primitive types - string vs int64",
			ExpectedType:     arrow.BinaryTypes.String,
			ActualType:       arrow.PrimitiveTypes.Int64,
			TypePath:         "root.field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.field: type IDs differ - expected: STRING, actual: INT64",
		},
		{
			Name:             "different primitive types - int64 vs float64",
			ExpectedType:     arrow.PrimitiveTypes.Int64,
			ActualType:       arrow.PrimitiveTypes.Float64,
			TypePath:         "root.field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.field: type IDs differ - expected: INT64, actual: FLOAT64",
		},
		{
			Name:             "different primitive types - int32 vs int64",
			ExpectedType:     arrow.PrimitiveTypes.Int32,
			ActualType:       arrow.PrimitiveTypes.Int64,
			TypePath:         "root.field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.field: type IDs differ - expected: INT32, actual: INT64",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestCompareArrowTypes_TimestampTypes(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name:          "identical timestamp types - nanoseconds",
			ExpectedType:  arrow.FixedWidthTypes.Timestamp_ns,
			ActualType:    arrow.FixedWidthTypes.Timestamp_ns,
			TypePath:      "root.timestamp",
			ExpectedEqual: true,
		},
		{
			Name:          "identical timestamp types - microseconds",
			ExpectedType:  arrow.FixedWidthTypes.Timestamp_us,
			ActualType:    arrow.FixedWidthTypes.Timestamp_us,
			TypePath:      "root.timestamp",
			ExpectedEqual: true,
		},
		{
			Name:             "different timestamp units",
			ExpectedType:     arrow.FixedWidthTypes.Timestamp_ns,
			ActualType:       arrow.FixedWidthTypes.Timestamp_us,
			TypePath:         "root.timestamp",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.timestamp: timestamp units differ - expected: ns, actual: us",
		},
		{
			Name: "identical timestamp with timezone",
			ExpectedType: &arrow.TimestampType{
				Unit:     arrow.Nanosecond,
				TimeZone: "UTC",
			},
			ActualType: &arrow.TimestampType{
				Unit:     arrow.Nanosecond,
				TimeZone: "UTC",
			},
			TypePath:      "root.timestamp",
			ExpectedEqual: true,
		},
		{
			Name: "different timestamp timezones",
			ExpectedType: &arrow.TimestampType{
				Unit:     arrow.Nanosecond,
				TimeZone: "UTC",
			},
			ActualType: &arrow.TimestampType{
				Unit:     arrow.Nanosecond,
				TimeZone: "America/New_York",
			},
			TypePath:         "root.timestamp",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.timestamp: timestamp timezones differ - expected: UTC, actual: America/New_York",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestCompareArrowTypes_ListTypes(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name:          "identical list of strings",
			ExpectedType:  arrow.ListOf(arrow.BinaryTypes.String),
			ActualType:    arrow.ListOf(arrow.BinaryTypes.String),
			TypePath:      "root.list_field",
			ExpectedEqual: true,
		},
		{
			Name:          "identical list of int64",
			ExpectedType:  arrow.ListOf(arrow.PrimitiveTypes.Int64),
			ActualType:    arrow.ListOf(arrow.PrimitiveTypes.Int64),
			TypePath:      "root.list_field",
			ExpectedEqual: true,
		},
		{
			Name:             "different list element types",
			ExpectedType:     arrow.ListOf(arrow.BinaryTypes.String),
			ActualType:       arrow.ListOf(arrow.PrimitiveTypes.Int64),
			TypePath:         "root.list_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.list_field[LIST_ELEMENT]: type IDs differ - expected: STRING, actual: INT64",
		},
		{
			Name:          "nested list comparison - identical",
			ExpectedType:  arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String)),
			ActualType:    arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String)),
			TypePath:      "root.nested_list",
			ExpectedEqual: true,
		},
		{
			Name:             "nested list comparison - different inner type",
			ExpectedType:     arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String)),
			ActualType:       arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)),
			TypePath:         "root.nested_list",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.nested_list[LIST_ELEMENT][LIST_ELEMENT]: type IDs differ - expected: STRING, actual: INT64",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestCompareArrowTypes_StructTypes(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name: "identical simple struct",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			),
			TypePath:      "root.struct_field",
			ExpectedEqual: true,
		},
		{
			Name: "different struct field count",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			),
			TypePath:         "root.struct_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.struct_field: struct field counts differ - expected: 2, actual: 1",
		},
		{
			Name: "different struct field names",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "username", Type: arrow.BinaryTypes.String, Nullable: true},
			),
			TypePath:         "root.struct_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.struct_field.STRUCT[0]: field names differ - expected: name, actual: username",
		},
		{
			Name: "different struct field nullability",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
			),
			TypePath:         "root.struct_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.struct_field.STRUCT[0].name: nullability differs - expected: true, actual: false",
		},
		{
			Name: "different struct field types",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			),
			TypePath:         "root.struct_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.struct_field.STRUCT[0].value: type IDs differ - expected: STRING, actual: INT64",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestCompareArrowTypes_NestedComplexTypes(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name: "identical nested struct with list",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "user", Type: arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
				), Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "user", Type: arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
				), Nullable: true},
			),
			TypePath:      "root.complex_field",
			ExpectedEqual: true,
		},
		{
			Name: "list of structs - identical",
			ExpectedType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
			)),
			ActualType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
			)),
			TypePath:      "root.properties",
			ExpectedEqual: true,
		},
		{
			Name: "nested difference - struct field in list element",
			ExpectedType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
			)),
			ActualType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			)),
			TypePath:         "root.properties",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.properties[LIST_ELEMENT].STRUCT[1].value: type IDs differ - expected: STRING, actual: INT64",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestCompareArrowTypes_EdgeCases(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name:          "empty struct comparison",
			ExpectedType:  arrow.StructOf(),
			ActualType:    arrow.StructOf(),
			TypePath:      "root.empty_struct",
			ExpectedEqual: true,
		},
		{
			Name:             "empty struct vs single field struct",
			ExpectedType:     arrow.StructOf(),
			ActualType:       arrow.StructOf(arrow.Field{Name: "field", Type: arrow.BinaryTypes.String, Nullable: true}),
			TypePath:         "root.struct_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.struct_field: struct field counts differ - expected: 0, actual: 1",
		},
		{
			Name:          "deeply nested identical types",
			ExpectedType:  arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String))),
			ActualType:    arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String))),
			TypePath:      "root.deep_nested",
			ExpectedEqual: true,
		},
		{
			Name:          "deeply nested different types",
			ExpectedType:  arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String))),
			ActualType:    arrow.ListOf(arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))),
			TypePath:      "root.deep_nested",
			ExpectedEqual: false,
			ExpectedErrorMsg: "root.deep_nested[LIST_ELEMENT][LIST_ELEMENT][LIST_ELEMENT]: " +
				"type IDs differ - expected: STRING, actual: INT64",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestCompareArrowTypes_TypePathTracking(t *testing.T) {
	// given
	testCases := []struct {
		Name         string
		ExpectedType arrow.DataType
		ActualType   arrow.DataType
		TypePath     string
		ExpectedPath string
	}{
		{
			Name:         "simple type path",
			ExpectedType: arrow.BinaryTypes.String,
			ActualType:   arrow.PrimitiveTypes.Int64,
			TypePath:     "table.field",
			ExpectedPath: "table.field",
		},
		{
			Name:         "nested type path",
			ExpectedType: arrow.BinaryTypes.String,
			ActualType:   arrow.PrimitiveTypes.Int64,
			TypePath:     "table.user.profile.name",
			ExpectedPath: "table.user.profile.name",
		},
		{
			Name:         "complex type path",
			ExpectedType: arrow.BinaryTypes.String,
			ActualType:   arrow.PrimitiveTypes.Int64,
			TypePath:     "schema.table[0].struct.field",
			ExpectedPath: "schema.table[0].struct.field",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.False(t, result.Equal)
			assert.Equal(t, tc.ExpectedPath, result.TypePath)
			assert.Contains(t, result.ErrorMessage, tc.TypePath)
		})
	}
}

func TestCompareArrowTypes_ComplexNestedScenarios(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name: "deeply nested struct differences - 3 levels deep",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "level1", Type: arrow.StructOf(
					arrow.Field{Name: "level2", Type: arrow.StructOf(
						arrow.Field{Name: "level3", Type: arrow.BinaryTypes.String, Nullable: true},
					), Nullable: true},
				), Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "level1", Type: arrow.StructOf(
					arrow.Field{Name: "level2", Type: arrow.StructOf(
						arrow.Field{Name: "level3", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					), Nullable: true},
				), Nullable: true},
			),
			TypePath:      "root.deep_struct",
			ExpectedEqual: false,
			ExpectedErrorMsg: "root.deep_struct.STRUCT[0].level1.STRUCT[0].level2.STRUCT[0].level3: " +
				"type IDs differ - expected: STRING, actual: INT64",
		},
		{
			Name: "list of structs with list fields",
			ExpectedType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
				arrow.Field{Name: "metadata", Type: arrow.StructOf(
					arrow.Field{Name: "created", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
					arrow.Field{Name: "categories", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
				), Nullable: true},
			)),
			ActualType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
				arrow.Field{Name: "metadata", Type: arrow.StructOf(
					arrow.Field{Name: "created", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
					arrow.Field{Name: "categories", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
				), Nullable: true},
			)),
			TypePath:      "root.complex_list",
			ExpectedEqual: true,
		},
		{
			Name: "struct field order matters",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "first", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "second", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "second", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				arrow.Field{Name: "first", Type: arrow.BinaryTypes.String, Nullable: true},
			),
			TypePath:         "root.ordered_struct",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.ordered_struct.STRUCT[0]: field names differ - expected: first, actual: second",
		},
		{
			Name: "multiple timestamp types with different configurations",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "created_ns", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
				arrow.Field{Name: "updated_us", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
				arrow.Field{Name: "scheduled_utc", Type: &arrow.TimestampType{
					Unit:     arrow.Millisecond,
					TimeZone: "UTC",
				}, Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "created_ns", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
				arrow.Field{Name: "updated_us", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
				arrow.Field{Name: "scheduled_utc", Type: &arrow.TimestampType{
					Unit:     arrow.Millisecond,
					TimeZone: "UTC",
				}, Nullable: true},
			),
			TypePath:      "root.timestamps",
			ExpectedEqual: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestCompareArrowTypes_SpecialDataTypes(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name:          "identical binary types",
			ExpectedType:  arrow.BinaryTypes.Binary,
			ActualType:    arrow.BinaryTypes.Binary,
			TypePath:      "root.binary_field",
			ExpectedEqual: true,
		},
		{
			Name:             "binary vs string types",
			ExpectedType:     arrow.BinaryTypes.Binary,
			ActualType:       arrow.BinaryTypes.String,
			TypePath:         "root.binary_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.binary_field: type IDs differ - expected: BINARY, actual: STRING",
		},
		{
			Name: "complex timestamp comparison with different units and timezones",
			ExpectedType: &arrow.TimestampType{
				Unit:     arrow.Millisecond,
				TimeZone: "America/New_York",
			},
			ActualType: &arrow.TimestampType{
				Unit:     arrow.Second,
				TimeZone: "America/New_York",
			},
			TypePath:         "root.timestamp_complex",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.timestamp_complex: timestamp units differ - expected: ms, actual: s",
		},
		{
			Name:             "list of different primitive types",
			ExpectedType:     arrow.ListOf(arrow.PrimitiveTypes.Float32),
			ActualType:       arrow.ListOf(arrow.PrimitiveTypes.Float64),
			TypePath:         "root.float_list",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.float_list[LIST_ELEMENT]: type IDs differ - expected: FLOAT32, actual: FLOAT64",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestCompareArrowTypes_ErrorMessagePrecision(t *testing.T) {
	// given - test that error messages are precise and helpful
	testCases := []struct {
		Name             string
		ExpectedType     arrow.DataType
		ActualType       arrow.DataType
		TypePath         string
		ExpectedContains []string
	}{
		{
			Name: "nested struct field type mismatch should show exact path",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "user", Type: arrow.StructOf(
					arrow.Field{Name: "profile", Type: arrow.StructOf(
						arrow.Field{Name: "avatar_url", Type: arrow.BinaryTypes.String, Nullable: true},
					), Nullable: true},
				), Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "user", Type: arrow.StructOf(
					arrow.Field{Name: "profile", Type: arrow.StructOf(
						arrow.Field{Name: "avatar_url", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					), Nullable: true},
				), Nullable: true},
			),
			TypePath: "schema.events",
			ExpectedContains: []string{
				"schema.events.STRUCT[0].user.STRUCT[0].profile.STRUCT[0].avatar_url",
				"type IDs differ",
				"expected: STRING",
				"actual: INT64",
			},
		},
		{
			Name: "list element mismatch should show list element path",
			ExpectedType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
			)),
			ActualType: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			)),
			TypePath: "table.items",
			ExpectedContains: []string{
				"table.items[LIST_ELEMENT].STRUCT[0].id",
				"type IDs differ",
				"expected: STRING",
				"actual: INT64",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			result := CompareArrowTypes(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.False(t, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)

			for _, expectedText := range tc.ExpectedContains {
				assert.Contains(t, result.ErrorMessage, expectedText,
					"Error message should contain '%s'\nActual message: %s",
					expectedText, result.ErrorMessage)
			}
		})
	}
}

func TestTypeComparer_DefaultBehavior(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name:          "identical types should match",
			ExpectedType:  arrow.PrimitiveTypes.Int64,
			ActualType:    arrow.PrimitiveTypes.Int64,
			TypePath:      "root.field",
			ExpectedEqual: true,
		},
		{
			Name:             "different types should not match",
			ExpectedType:     arrow.PrimitiveTypes.Int32,
			ActualType:       arrow.PrimitiveTypes.Int64,
			TypePath:         "root.field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.field: type IDs differ - expected: INT32, actual: INT64",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// given
			comparer := NewTypeComparer()

			// when
			result := comparer.Compare(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func exampleCompatibilityRule(expected, actual arrow.DataType) (compatible, handled bool) {
	if expected.ID() == arrow.INT32 && actual.ID() == arrow.INT64 {
		return true, true
	}
	if expected.ID() == arrow.INT64 && actual.ID() == arrow.INT32 {
		return true, true
	}
	return false, false
}

func TestTypeComparer_ExampleCompatibilityRule(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name:          "int32 expected, int64 actual - should be compatible",
			ExpectedType:  arrow.PrimitiveTypes.Int32,
			ActualType:    arrow.PrimitiveTypes.Int64,
			TypePath:      "root.example_field",
			ExpectedEqual: true,
		},
		{
			Name:          "int64 expected, int32 actual - should be compatible",
			ExpectedType:  arrow.PrimitiveTypes.Int64,
			ActualType:    arrow.PrimitiveTypes.Int32,
			TypePath:      "root.example_field",
			ExpectedEqual: true,
		},
		{
			Name:          "int32 expected, int32 actual - should be compatible",
			ExpectedType:  arrow.PrimitiveTypes.Int32,
			ActualType:    arrow.PrimitiveTypes.Int32,
			TypePath:      "root.example_field",
			ExpectedEqual: true,
		},
		{
			Name:          "int64 expected, int64 actual - should be compatible",
			ExpectedType:  arrow.PrimitiveTypes.Int64,
			ActualType:    arrow.PrimitiveTypes.Int64,
			TypePath:      "root.example_field",
			ExpectedEqual: true,
		},
		{
			Name:             "int32 expected, string actual - should not be compatible",
			ExpectedType:     arrow.PrimitiveTypes.Int32,
			ActualType:       arrow.BinaryTypes.String,
			TypePath:         "root.example_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.example_field: type IDs differ - expected: INT32, actual: STRING",
		},
		{
			Name:             "string expected, int64 actual - should not be compatible",
			ExpectedType:     arrow.BinaryTypes.String,
			ActualType:       arrow.PrimitiveTypes.Int64,
			TypePath:         "root.example_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.example_field: type IDs differ - expected: STRING, actual: INT64",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// given
			comparer := NewTypeComparer(exampleCompatibilityRule)

			// when
			result := comparer.Compare(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestTypeComparer_ExampleCompatibilityInNestedStructures(t *testing.T) {
	// given
	testCases := []TypeComparisonTestCase{
		{
			Name: "struct with int32/int64 compatibility",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
				arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
				arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			),
			TypePath:      "root.struct_field",
			ExpectedEqual: true,
		},
		{
			Name:          "list of int32 vs list of int64 - should be compatible",
			ExpectedType:  arrow.ListOf(arrow.PrimitiveTypes.Int32),
			ActualType:    arrow.ListOf(arrow.PrimitiveTypes.Int64),
			TypePath:      "root.list_field",
			ExpectedEqual: true,
		},
		{
			Name: "nested struct with mixed compatibility",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "user", Type: arrow.StructOf(
					arrow.Field{Name: "user_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
					arrow.Field{Name: "metadata", Type: arrow.StructOf(
						arrow.Field{Name: "version", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					), Nullable: true},
				), Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "user", Type: arrow.StructOf(
					arrow.Field{Name: "user_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
					arrow.Field{Name: "metadata", Type: arrow.StructOf(
						arrow.Field{Name: "version", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
					), Nullable: true},
				), Nullable: true},
			),
			TypePath:      "root.complex_field",
			ExpectedEqual: true,
		},
		{
			Name: "mixed compatibility failure - int32 vs string should still fail",
			ExpectedType: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			),
			ActualType: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			),
			TypePath:         "root.struct_field",
			ExpectedEqual:    false,
			ExpectedErrorMsg: "root.struct_field.STRUCT[0].id: type IDs differ - expected: INT32, actual: STRING",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// given
			comparer := NewTypeComparer(exampleCompatibilityRule)

			// when
			result := comparer.Compare(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
			if !tc.ExpectedEqual {
				assert.Contains(t, result.ErrorMessage, tc.ExpectedErrorMsg)
			} else {
				assert.Empty(t, result.ErrorMessage)
			}
		})
	}
}

func TestTypeComparer_MultipleCompatibilityRules(t *testing.T) {
	// given - custom rule that makes strings compatible with binary
	stringBinaryCompatibilityRule := func(expected, actual arrow.DataType) (compatible bool, handled bool) {
		if expected.ID() == arrow.STRING && actual.ID() == arrow.BINARY {
			return true, true
		}
		if expected.ID() == arrow.BINARY && actual.ID() == arrow.STRING {
			return true, true
		}
		return false, false
	}

	testCases := []struct {
		Name          string
		ExpectedType  arrow.DataType
		ActualType    arrow.DataType
		TypePath      string
		ExpectedEqual bool
	}{
		{
			Name:          "int32 vs int64 - Example rule should handle",
			ExpectedType:  arrow.PrimitiveTypes.Int32,
			ActualType:    arrow.PrimitiveTypes.Int64,
			TypePath:      "root.field",
			ExpectedEqual: true,
		},
		{
			Name:          "string vs binary - custom rule should handle",
			ExpectedType:  arrow.BinaryTypes.String,
			ActualType:    arrow.BinaryTypes.Binary,
			TypePath:      "root.field",
			ExpectedEqual: true,
		},
		{
			Name:          "binary vs string - custom rule should handle",
			ExpectedType:  arrow.BinaryTypes.Binary,
			ActualType:    arrow.BinaryTypes.String,
			TypePath:      "root.field",
			ExpectedEqual: true,
		},
		{
			Name:          "int32 vs string - no rule handles, should fail",
			ExpectedType:  arrow.PrimitiveTypes.Int32,
			ActualType:    arrow.BinaryTypes.String,
			TypePath:      "root.field",
			ExpectedEqual: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// given
			comparer := NewTypeComparer(exampleCompatibilityRule, stringBinaryCompatibilityRule)

			// when
			result := comparer.Compare(tc.ExpectedType, tc.ActualType, tc.TypePath)

			// then
			assert.Equal(t, tc.ExpectedEqual, result.Equal)
			assert.Equal(t, tc.TypePath, result.TypePath)
		})
	}
}

func TestExampleCompatibilityRule_Standalone(t *testing.T) {
	// given
	testCases := []struct {
		Name               string
		ExpectedType       arrow.DataType
		ActualType         arrow.DataType
		ExpectedCompatible bool
		ExpectedHandled    bool
	}{
		{
			Name:               "int32 vs int64",
			ExpectedType:       arrow.PrimitiveTypes.Int32,
			ActualType:         arrow.PrimitiveTypes.Int64,
			ExpectedCompatible: true,
			ExpectedHandled:    true,
		},
		{
			Name:               "int64 vs int32",
			ExpectedType:       arrow.PrimitiveTypes.Int64,
			ActualType:         arrow.PrimitiveTypes.Int32,
			ExpectedCompatible: true,
			ExpectedHandled:    true,
		},
		{
			Name:               "int32 vs int32",
			ExpectedType:       arrow.PrimitiveTypes.Int32,
			ActualType:         arrow.PrimitiveTypes.Int32,
			ExpectedCompatible: false,
			ExpectedHandled:    false,
		},
		{
			Name:               "int64 vs int64",
			ExpectedType:       arrow.PrimitiveTypes.Int64,
			ActualType:         arrow.PrimitiveTypes.Int64,
			ExpectedCompatible: false,
			ExpectedHandled:    false,
		},
		{
			Name:               "string vs int64",
			ExpectedType:       arrow.BinaryTypes.String,
			ActualType:         arrow.PrimitiveTypes.Int64,
			ExpectedCompatible: false,
			ExpectedHandled:    false,
		},
		{
			Name:               "int32 vs string",
			ExpectedType:       arrow.PrimitiveTypes.Int32,
			ActualType:         arrow.BinaryTypes.String,
			ExpectedCompatible: false,
			ExpectedHandled:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// when
			compatible, handled := exampleCompatibilityRule(tc.ExpectedType, tc.ActualType)

			// then
			assert.Equal(t, tc.ExpectedCompatible, compatible)
			assert.Equal(t, tc.ExpectedHandled, handled)
		})
	}
}
