package files

import (
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetPrimitiveMappers_SupportedAndUnsupported(t *testing.T) {
	tests := []struct {
		name       string
		mapper     warehouse.FieldTypeMapper[SpecificParquetType]
		arrowType  arrow.DataType
		badType    arrow.DataType
		nodeAssert func(t *testing.T, node any)
	}{
		{
			name:      "string",
			mapper:    &parquetStringTypeMapper{},
			arrowType: arrow.BinaryTypes.String,
			badType:   arrow.PrimitiveTypes.Int64,
		},
		{
			name:      "int64",
			mapper:    &parquetInt64TypeMapper{},
			arrowType: arrow.PrimitiveTypes.Int64,
			badType:   arrow.BinaryTypes.String,
		},
		{
			name:      "int32",
			mapper:    &parquetInt32TypeMapper{},
			arrowType: arrow.PrimitiveTypes.Int32,
			badType:   arrow.BinaryTypes.String,
		},
		{
			name:      "float64",
			mapper:    &parquetFloat64TypeMapper{},
			arrowType: arrow.PrimitiveTypes.Float64,
			badType:   arrow.BinaryTypes.String,
		},
		{
			name:      "float32",
			mapper:    &parquetFloat32TypeMapper{},
			arrowType: arrow.PrimitiveTypes.Float32,
			badType:   arrow.BinaryTypes.String,
		},
		{
			name:      "bool",
			mapper:    &parquetBoolTypeMapper{},
			arrowType: arrow.FixedWidthTypes.Boolean,
			badType:   arrow.BinaryTypes.String,
		},
		{
			name:      "timestamp",
			mapper:    &parquetTimestampTypeMapper{},
			arrowType: arrow.FixedWidthTypes.Timestamp_s,
			badType:   arrow.BinaryTypes.String,
		},
		{
			name:      "date32",
			mapper:    &parquetDate32TypeMapper{},
			arrowType: arrow.FixedWidthTypes.Date32,
			badType:   arrow.BinaryTypes.String,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapped, err := tt.mapper.ArrowToWarehouse(warehouse.ArrowType{ArrowDataType: tt.arrowType})
			require.NoError(t, err)
			assert.NotNil(t, mapped.Node)

			_, err = tt.mapper.ArrowToWarehouse(warehouse.ArrowType{ArrowDataType: tt.badType})
			require.Error(t, err)
			assert.IsType(t, warehouse.NewUnsupportedMappingErr(tt.badType, parquetMapperName), err)
		})
	}
}

func TestParquetIntegerCoercionFromFloat64(t *testing.T) {
	mapper := NewParquetFieldTypeMapper()

	int64Type, err := mapper.ArrowToWarehouse(warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int64})
	require.NoError(t, err)

	got, err := int64Type.Format(float64(123), arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, int64(123), got)

	_, err = int64Type.Format(float64(123.5), arrow.Metadata{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not an integer")

	_, err = int64Type.Format(float64(math.MaxFloat64), arrow.Metadata{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows int64 range")

	int32Type, err := mapper.ArrowToWarehouse(warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int32})
	require.NoError(t, err)

	got, err = int32Type.Format(float64(456), arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, int32(456), got)

	_, err = int32Type.Format(float64(math.MaxInt32)+1, arrow.Metadata{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows int32 range")
}

func TestParquetFloatCoercion(t *testing.T) {
	mapper := NewParquetFieldTypeMapper()

	float64Type, err := mapper.ArrowToWarehouse(warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float64})
	require.NoError(t, err)

	got64, err := float64Type.Format(float64(12.5), arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, float64(12.5), got64)

	float32Type, err := mapper.ArrowToWarehouse(warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float32})
	require.NoError(t, err)

	got32, err := float32Type.Format(float64(12.5), arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, float32(12.5), got32)

	_, err = float32Type.Format(float64(math.MaxFloat64), arrow.Metadata{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows float32 range")
}

func TestParquetTimestampFormatting(t *testing.T) {
	mapper := NewParquetFieldTypeMapper()
	timestampType, err := mapper.ArrowToWarehouse(warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Timestamp_s})
	require.NoError(t, err)

	tFromString, err := timestampType.Format("2026-02-24T14:30:45Z", arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 2, 24, 14, 30, 45, 0, time.UTC), tFromString)

	now := time.Date(2026, 2, 24, 14, 30, 45, 123, time.UTC)
	tFromTime, err := timestampType.Format(now, arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, now.UTC(), tFromTime)

	tFromUnix, err := timestampType.Format(float64(1700000000), arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, time.Unix(1700000000, 0).UTC(), tFromUnix)
}

func TestParquetDate32Formatting(t *testing.T) {
	mapper := NewParquetFieldTypeMapper()
	dateType, err := mapper.ArrowToWarehouse(warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Date32})
	require.NoError(t, err)

	fromString, err := dateType.Format("2026-02-24", arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 2, 24, 0, 0, 0, 0, time.UTC), fromString)

	fromTimeValue := time.Date(2026, 2, 24, 12, 30, 0, 0, time.FixedZone("utc+2", 2*3600))
	fromTime, err := dateType.Format(fromTimeValue, arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 2, 24, 0, 0, 0, 0, time.UTC), fromTime)
}

func TestParquetNullableWrapper(t *testing.T) {
	mapper := NewParquetFieldTypeMapper()
	mapped, err := mapper.ArrowToWarehouse(warehouse.ArrowType{
		ArrowDataType: arrow.BinaryTypes.String,
		Nullable:      true,
	})
	require.NoError(t, err)
	assert.True(t, mapped.Node.Optional())

	got, err := mapped.Format(nil, arrow.Metadata{})
	require.NoError(t, err)
	assert.Nil(t, got)

	got, err = mapped.Format("value", arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, "value", got)
}

func TestParquetArrayMapperFormatting(t *testing.T) {
	mapper := NewParquetFieldTypeMapper()
	mapped, err := mapper.ArrowToWarehouse(warehouse.ArrowType{
		ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int64),
	})
	require.NoError(t, err)

	got, err := mapped.Format([]any{float64(1), float64(2)}, arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, []any{int64(1), int64(2)}, got)

	_, err = mapped.Format(nil, arrow.Metadata{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected []any for array, got <nil>")
}

func TestParquetNullableListWrapperAppliesToTopLevelList(t *testing.T) {
	mapper := NewParquetFieldTypeMapper()
	mapped, err := mapper.ArrowToWarehouse(warehouse.ArrowType{
		ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Int64),
		Nullable:      true,
	})
	require.NoError(t, err)
	assert.True(t, mapped.Node.Optional())

	got, err := mapped.Format(nil, arrow.Metadata{})
	require.NoError(t, err)
	assert.Nil(t, got)

	got, err = mapped.Format([]any{float64(3), float64(4)}, arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, []any{int64(3), int64(4)}, got)
}

func TestParquetArrayMapper_NullableStructElementIncreasesDefinitionLevel(t *testing.T) {
	mapper := NewParquetFieldTypeMapper()
	listWithNullableStructElement := arrow.ListOfField(arrow.Field{
		Name: "element",
		Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
		),
		Nullable: true,
	})
	listWithRequiredStructElement := arrow.ListOfField(arrow.Field{
		Name: "element",
		Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
		),
		Nullable: false,
	})

	nullableSchema, _, err := buildParquetSchema(
		arrow.NewSchema([]arrow.Field{{Name: "items", Type: listWithNullableStructElement}}, nil),
		mapper,
	)
	require.NoError(t, err)
	requiredSchema, _, err := buildParquetSchema(
		arrow.NewSchema([]arrow.Field{{Name: "items", Type: listWithRequiredStructElement}}, nil),
		mapper,
	)
	require.NoError(t, err)

	nullableElementLeaf, ok := nullableSchema.Lookup("items", "list", "element", "name")
	require.True(t, ok)
	requiredElementLeaf, ok := requiredSchema.Lookup("items", "list", "element", "name")
	require.True(t, ok)
	assert.Greater(t, nullableElementLeaf.MaxDefinitionLevel, requiredElementLeaf.MaxDefinitionLevel)
}

func TestParquetNestedMapperMetadataAndNestingValidation(t *testing.T) {
	nestedMapper := &parquetNestedTypeMapper{SubMapper: NewParquetFieldTypeMapper()}

	structType := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	)

	_, err := nestedMapper.ArrowToWarehouse(warehouse.ArrowType{ArrowDataType: structType})
	require.Error(t, err)
	assert.IsType(t, warehouse.NewUnsupportedMappingErr(structType, parquetMapperName), err)

	mapped, err := nestedMapper.ArrowToWarehouse(warehouse.ArrowType{
		ArrowDataType: structType,
		Metadata:      arrow.NewMetadata([]string{parquetMetadataKeyPT}, []string{"array"}),
	})
	require.NoError(t, err)
	require.NotNil(t, mapped.Node)

	formatted, err := mapped.Format(map[string]any{"name": "abc", "value": float64(9)}, arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"name": "abc", "value": int64(9)}, formatted)

	deepStruct := arrow.StructOf(
		arrow.Field{Name: "nested", Type: arrow.StructOf(arrow.Field{Name: "x", Type: arrow.BinaryTypes.String})},
	)

	_, err = nestedMapper.ArrowToWarehouse(warehouse.ArrowType{
		ArrowDataType: deepStruct,
		Metadata:      arrow.NewMetadata([]string{parquetMetadataKeyPT}, []string{"array"}),
	})
	require.Error(t, err)
	assert.IsType(t, warehouse.NewUnsupportedMappingErr(deepStruct.Fields()[0].Type, parquetMapperName), err)
}

func TestBuildParquetSchema_MixedFieldsAndOrderedFuncs(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64},
		{Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
	}, nil)

	pqSchema, formatFuncs, err := buildParquetSchema(schema, NewParquetFieldTypeMapper())
	require.NoError(t, err)
	require.NotNil(t, pqSchema)
	assert.Equal(t, 4, len(formatFuncs))

	_, ok := pqSchema.Lookup("name")
	assert.True(t, ok)
	_, ok = pqSchema.Lookup("count")
	assert.True(t, ok)
	_, ok = pqSchema.Lookup("items", "list", "element")
	assert.True(t, ok)

	v0, err := formatFuncs[0]("a", arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, "a", v0)

	v1, err := formatFuncs[1](float64(7), arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, int64(7), v1)

	v2, err := formatFuncs[2]([]any{float64(1.25)}, arrow.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, []any{float32(1.25)}, v2)

	v3, err := formatFuncs[3](nil, arrow.Metadata{})
	require.NoError(t, err)
	assert.Nil(t, v3)
}
