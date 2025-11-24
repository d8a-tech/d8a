package bigquery

import (
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
)

// MapperName is the name of the BigQuery type mapper
const MapperName = "bigquery"
const metadataParentType = "Bigquery.ParentType"

// SpecificBigQueryType represents a BigQuery data type with its string representation and formatting function
type SpecificBigQueryType struct {
	FieldType  bigquery.FieldType
	Required   bool
	Repeated   bool
	FormatFunc func(SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error)
	// Schema holds the nested schema for RECORD types, nil for primitive types
	Schema *bigquery.Schema
}

// Format formats a value according to the BigQuery type's formatting function
func (t SpecificBigQueryType) Format(i any, m arrow.Metadata) (any, error) {
	return t.FormatFunc(t)(i, m)
}

// === PRIMITIVE TYPE MAPPERS ===

type bigQueryStringTypeMapper struct{}

func (m *bigQueryStringTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	if arrowType.ArrowDataType != arrow.BinaryTypes.String {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}
	return bigQueryString, nil
}

func (m *bigQueryStringTypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.FieldType != bigquery.StringFieldType {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.BinaryTypes.String}, nil
}

type bigQueryInt64TypeMapper struct{}

func (m *bigQueryInt64TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Int64 {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}
	return bigQueryInt64, nil
}

func (m *bigQueryInt64TypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.FieldType != bigquery.IntegerFieldType {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int64}, nil
}

type bigQueryFloat64TypeMapper struct{}

func (m *bigQueryFloat64TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Float64 {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}
	return bigQueryFloat64, nil
}

func (m *bigQueryFloat64TypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.FieldType != bigquery.FloatFieldType {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float64}, nil
}

type bigQueryBoolTypeMapper struct{}

func (m *bigQueryBoolTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	if arrowType.ArrowDataType != arrow.FixedWidthTypes.Boolean {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}
	return bigQueryBool, nil
}

func (m *bigQueryBoolTypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.FieldType != bigquery.BooleanFieldType {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Boolean}, nil
}

type bigQueryInt32TypeMapper struct{}

func (m *bigQueryInt32TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Int32 {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}
	return bigQueryInt32, nil
}

func (m *bigQueryInt32TypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.FieldType != bigquery.IntegerFieldType {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int32}, nil
}

type bigQueryFloat32TypeMapper struct{}

func (m *bigQueryFloat32TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Float32 {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}
	return bigQueryFloat32, nil
}

func (m *bigQueryFloat32TypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.FieldType != bigquery.FloatFieldType {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float32}, nil
}

type bigQueryTimestampTypeMapper struct{}

func (m *bigQueryTimestampTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	// Check if it's a timestamp type (any precision)
	switch t := arrowType.ArrowDataType.(type) {
	case *arrow.TimestampType:
		if t.Unit != arrow.Second {
			return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
		}
		return bigQueryTimestamp, nil
	default:
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}
}

func (m *bigQueryTimestampTypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.FieldType != bigquery.TimestampFieldType {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}
	// Use second precision
	return warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Timestamp_s}, nil
}

type bigQueryDate32TypeMapper struct{}

func (m *bigQueryDate32TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	if arrowType.ArrowDataType != arrow.FixedWidthTypes.Date32 {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}
	return bigQueryDate32, nil
}

func (m *bigQueryDate32TypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.FieldType != bigquery.DateFieldType {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Date32}, nil
}

// === COMPLEX TYPE MAPPERS ===

type bigQueryArrayTypeMapper struct {
	SubMapper warehouse.FieldTypeMapper[SpecificBigQueryType]
}

func (m *bigQueryArrayTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	listType, ok := arrowType.ArrowDataType.(*arrow.ListType)
	if !ok {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}

	// Map the element type
	elementArrowType := warehouse.ArrowType{
		ArrowDataType: listType.Elem(),
		Nullable:      listType.ElemField().Nullable,
		Metadata:      arrow.NewMetadata([]string{metadataParentType}, []string{"array"}),
	}
	elementType, err := m.SubMapper.ArrowToWarehouse(elementArrowType)
	if err != nil {
		return SpecificBigQueryType{}, err
	}

	return SpecificBigQueryType{
		FieldType: elementType.FieldType,
		Required:  false,
		Repeated:  true,
		Schema:    elementType.Schema,
		FormatFunc: func(_ SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
			return func(i any, metadata arrow.Metadata) (any, error) {
				slice, ok := i.([]any)
				if !ok {
					return nil, fmt.Errorf("expected []any for array, got %T", i)
				}

				result := make([]any, len(slice))
				for idx, elem := range slice {
					formatted, err := elementType.Format(elem, metadata)
					if err != nil {
						return nil, fmt.Errorf("error formatting array element at index %d: %w", idx, err)
					}
					result[idx] = formatted
				}
				return result, nil
			}
		},
	}, nil
}

func (m *bigQueryArrayTypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	// In BigQuery, arrays are represented by the Repeated field being true
	if !warehouseType.Repeated {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}

	// Create a non-repeated version of the type to get the element type
	elementType := SpecificBigQueryType{
		FieldType:  warehouseType.FieldType,
		Required:   warehouseType.Required,
		Repeated:   false,
		Schema:     warehouseType.Schema, // Copy schema for RECORD types
		FormatFunc: warehouseType.FormatFunc,
	}

	// Map the element type back to Arrow
	elementArrowType, err := m.SubMapper.WarehouseToArrow(elementType)
	if err != nil {
		return warehouse.ArrowType{}, err
	}

	// Create the ListType with proper element field nullable setting
	elementField := arrow.Field{
		Name:     "item",
		Type:     elementArrowType.ArrowDataType,
		Nullable: elementArrowType.Nullable,
	}

	return warehouse.ArrowType{
		ArrowDataType: arrow.ListOfField(elementField),
		Nullable:      !warehouseType.Required,
	}, nil
}

type bigQueryNestedTypeMapper struct {
	SubMapper warehouse.FieldTypeMapper[SpecificBigQueryType]
}

func (m *bigQueryNestedTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	structType, ok := arrowType.ArrowDataType.(*arrow.StructType)
	if !ok {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}

	// If this is a direct Struct (not inside an array), reject it
	if getParentType(arrowType) != "array" {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}

	for _, field := range structType.Fields() {
		if err := m.validatePrimitiveField(field.Type); err != nil {
			return SpecificBigQueryType{}, err
		}
	}

	fieldTypes := make([]SpecificBigQueryType, 0, len(structType.Fields()))
	schema := make(bigquery.Schema, 0, len(structType.Fields()))

	for _, field := range structType.Fields() {
		fieldArrowType := warehouse.ArrowType{
			ArrowDataType: field.Type,
			Nullable:      field.Nullable,
			Metadata:      arrow.NewMetadata([]string{metadataParentType}, []string{"struct"}),
		}
		fieldType, err := m.SubMapper.ArrowToWarehouse(fieldArrowType)
		if err != nil {
			return SpecificBigQueryType{}, fmt.Errorf("error mapping field %s: %w", field.Name, err)
		}
		fieldTypes = append(fieldTypes, fieldType)
		schemaField := &bigquery.FieldSchema{
			Name:     field.Name,
			Type:     fieldType.FieldType,
			Required: fieldType.Required,
			Repeated: fieldType.Repeated,
		}
		if fieldType.Schema != nil {
			schemaField.Schema = *fieldType.Schema
		}
		schema = append(schema, schemaField)
	}

	return SpecificBigQueryType{
		FieldType: bigquery.RecordFieldType,
		Required:  false,
		Repeated:  false,
		Schema:    &schema,
		FormatFunc: func(_ SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
			return func(i any, metadata arrow.Metadata) (any, error) {
				record, ok := i.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("expected map[string]any for nested type, got %T", i)
				}
				for idx, field := range structType.Fields() {
					if value, exists := record[field.Name]; exists {
						formatted, err := fieldTypes[idx].Format(value, metadata)
						if err != nil {
							return nil, fmt.Errorf("error formatting field %s: %w", field.Name, err)
						}
						record[field.Name] = formatted
					}
				}
				return record, nil
			}
		},
	}, nil
}

// validatePrimitiveField ensures that the field type is primitive (no nested Lists or Structs)
func (m *bigQueryNestedTypeMapper) validatePrimitiveField(dataType arrow.DataType) error {
	switch dataType.(type) {
	case *arrow.ListType:
		// Lists inside Struct are not allowed (multi-level nesting)
		return warehouse.NewUnsupportedMappingErr(dataType, MapperName)
	case *arrow.StructType:
		// Structs inside Struct are not allowed (multi-level nesting)
		return warehouse.NewUnsupportedMappingErr(dataType, MapperName)
	default:
		// Primitive types are allowed
		return nil
	}
}

func (m *bigQueryNestedTypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.FieldType != bigquery.RecordFieldType {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}

	if warehouseType.Schema == nil {
		return warehouse.ArrowType{}, fmt.Errorf("schema is required for BigQuery RECORD types")
	}

	// Convert BigQuery schema to Arrow fields
	arrowFields := make([]arrow.Field, 0, len(*warehouseType.Schema))
	for _, schemaField := range *warehouseType.Schema {
		// Create a SpecificBigQueryType for this field
		fieldBQType := SpecificBigQueryType{
			FieldType: schemaField.Type,
			Required:  schemaField.Required,
			Repeated:  schemaField.Repeated,
		}

		// For nested records, include the schema
		if len(schemaField.Schema) > 0 {
			fieldBQType.Schema = &schemaField.Schema
		}

		// Convert to Arrow type
		fieldArrowType, err := m.SubMapper.WarehouseToArrow(fieldBQType)
		if err != nil {
			return warehouse.ArrowType{}, fmt.Errorf("error converting field %s: %w", schemaField.Name, err)
		}

		arrowField := arrow.Field{
			Name:     schemaField.Name,
			Type:     fieldArrowType.ArrowDataType,
			Nullable: fieldArrowType.Nullable,
		}
		arrowFields = append(arrowFields, arrowField)
	}
	return warehouse.ArrowType{
		ArrowDataType: arrow.StructOf(arrowFields...),
		Nullable:      !warehouseType.Required,
	}, nil
}

type bigQueryNullableTypeMapper struct {
	SubMapper warehouse.FieldTypeMapper[SpecificBigQueryType]
}

func (m *bigQueryNullableTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificBigQueryType, error) {
	// For nullable types in BigQuery, we set Required to false
	if !arrowType.Nullable {
		return SpecificBigQueryType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, MapperName)
	}

	newInstance := arrowType.Copy()
	newInstance.Nullable = false
	innerType, err := m.SubMapper.ArrowToWarehouse(newInstance)
	if err != nil {
		return SpecificBigQueryType{}, err
	}

	return SpecificBigQueryType{
		FieldType: innerType.FieldType,
		Required:  false, // This makes it nullable in BigQuery
		Repeated:  innerType.Repeated,
		Schema:    innerType.Schema,
		FormatFunc: func(_ SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
			return func(i any, metadata arrow.Metadata) (any, error) {
				if i == nil {
					return nil, nil
				}
				return innerType.Format(i, metadata)
			}
		},
	}, nil
}

func (m *bigQueryNullableTypeMapper) WarehouseToArrow(
	warehouseType SpecificBigQueryType,
) (warehouse.ArrowType, error) {
	if warehouseType.Required {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, MapperName)
	}

	// Create a required version to get the inner type
	innerType := SpecificBigQueryType{
		FieldType:  warehouseType.FieldType,
		Required:   true,
		Repeated:   warehouseType.Repeated,
		Schema:     warehouseType.Schema,
		FormatFunc: warehouseType.FormatFunc,
	}

	// Map the inner type back to Arrow
	innerArrowType, err := m.SubMapper.WarehouseToArrow(innerType)
	if err != nil {
		return warehouse.ArrowType{}, err
	}

	// Return with nullable set to true
	innerArrowType.Nullable = true
	return innerArrowType, nil
}

// === TYPE INSTANCES ===

// createSimpleFormatFunc creates a format function that simply returns the value as is
func createSimpleFormatFunc() func(SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
	return func(_ SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
		return func(i any, _ arrow.Metadata) (any, error) {
			return i, nil
		}
	}
}

var bigQueryString = SpecificBigQueryType{
	FieldType:  bigquery.StringFieldType,
	Required:   true,
	Repeated:   false,
	FormatFunc: createSimpleFormatFunc(),
}

// createIntegerFormatFunc creates a format function for integer types
func createIntegerFormatFunc[T int32 | int64](
	targetType string,
) func(SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
	return func(_ SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
		return func(i any, _ arrow.Metadata) (any, error) {
			switch v := i.(type) {
			case int32:
				// Check if we're converting int32 to int64 (always safe)
				if targetType == "int64" {
					return T(v), nil
				}
				// Check if we're converting int32 to int32 (always safe)
				return T(v), nil
			case int64:
				// Check if we're converting int64 to int32 (might overflow)
				if targetType == "int32" {
					if v > int64(1<<31-1) || v < int64(-1<<31) {
						return nil, fmt.Errorf("int64 value %d overflows int32 range", v)
					}
				}
				return T(v), nil
			case T:
				return v, nil
			case int:
				// Check if we're converting int to int32 (might overflow on 64-bit systems)
				if targetType == "int32" {
					if v > int(1<<31-1) || v < int(-1<<31) {
						return nil, fmt.Errorf("int value %d overflows int32 range", v)
					}
				}
				return T(v), nil
			default:
				return nil, fmt.Errorf("expected %s-compatible type, got %T", targetType, i)
			}
		}
	}
}

var bigQueryInt64 = SpecificBigQueryType{
	FieldType:  bigquery.IntegerFieldType,
	Required:   true,
	Repeated:   false,
	Schema:     nil,
	FormatFunc: createIntegerFormatFunc[int64]("int64"),
}

// createFloatFormatFunc creates a format function for float types
func createFloatFormatFunc[T float32 | float64](
	targetType string,
) func(SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
	return func(_ SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
		return func(i any, _ arrow.Metadata) (any, error) {
			switch v := i.(type) {
			case float32:
				// Check if we're converting float32 to float64 (always safe)
				if targetType == "float64" {
					return T(v), nil
				}
				// Check if we're converting float32 to float32 (always safe)
				return T(v), nil
			case float64:
				// Check if we're converting float64 to float32 (might overflow)
				if targetType == "float32" {
					if v > float64(3.4028235e+38) || v < float64(-3.4028235e+38) {
						return nil, fmt.Errorf("float64 value %g overflows float32 range", v)
					}
				}
				return T(v), nil
			case T:
				return v, nil
			default:
				return nil, fmt.Errorf("expected %s-compatible type, got %T", targetType, i)
			}
		}
	}
}

var bigQueryFloat64 = SpecificBigQueryType{
	FieldType:  bigquery.FloatFieldType,
	Required:   true,
	Repeated:   false,
	FormatFunc: createFloatFormatFunc[float64]("float64"),
}

var bigQueryBool = SpecificBigQueryType{
	FieldType:  bigquery.BooleanFieldType,
	Required:   true,
	Repeated:   false,
	FormatFunc: createSimpleFormatFunc(),
}

var bigQueryInt32 = SpecificBigQueryType{
	FieldType:  bigquery.IntegerFieldType,
	Required:   true,
	Repeated:   false,
	Schema:     nil,
	FormatFunc: createIntegerFormatFunc[int32]("int32"),
}

var bigQueryFloat32 = SpecificBigQueryType{
	FieldType:  bigquery.FloatFieldType,
	Required:   true,
	Repeated:   false,
	FormatFunc: createFloatFormatFunc[float32]("float32"),
}

var bigQueryTimestamp = SpecificBigQueryType{
	FieldType:  bigquery.TimestampFieldType,
	Required:   true,
	Repeated:   false,
	FormatFunc: createSimpleFormatFunc(),
}

// createDate32FormatFunc creates a format function for Date32 types
func createDate32FormatFunc() func(SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
	return func(_ SpecificBigQueryType) func(i any, m arrow.Metadata) (any, error) {
		return func(i any, _ arrow.Metadata) (any, error) {
			switch v := i.(type) {
			case time.Time:
				// Format as YYYY-MM-DD for BigQuery DATE type
				return v.Format("2006-01-02"), nil
			case string:
				// Assume it's already in the correct format, but validate
				_, err := time.Parse("2006-01-02", v)
				if err != nil {
					return nil, fmt.Errorf("invalid date format, expected YYYY-MM-DD: %w", err)
				}
				return v, nil
			default:
				return nil, fmt.Errorf("expected time.Time or date string, got %T", i)
			}
		}
	}
}

var bigQueryDate32 = SpecificBigQueryType{
	FieldType:  bigquery.DateFieldType,
	Required:   true,
	Repeated:   false,
	FormatFunc: createDate32FormatFunc(),
}

func getParentType(arrowType warehouse.ArrowType) string {
	parentType := ""
	if arrowType.Metadata.Len() > 0 {
		for i := 0; i < arrowType.Metadata.Len(); i++ {
			if arrowType.Metadata.Keys()[i] == metadataParentType {
				parentType = arrowType.Metadata.Values()[i]
				break
			}
		}
	}

	return parentType
}

// NewFieldTypeMapper creates a mapper that supports BigQuery types
func NewFieldTypeMapper() warehouse.FieldTypeMapper[SpecificBigQueryType] {
	// Base primitive mappers
	baseMappers := []warehouse.FieldTypeMapper[SpecificBigQueryType]{
		&bigQueryStringTypeMapper{},
		&bigQueryInt64TypeMapper{},
		&bigQueryFloat64TypeMapper{},
		&bigQueryBoolTypeMapper{},
		&bigQueryInt32TypeMapper{},
		&bigQueryFloat32TypeMapper{},
		&bigQueryBoolTypeMapper{},
		&bigQueryTimestampTypeMapper{},
		&bigQueryDate32TypeMapper{},
	}

	// deferred mapper for circular dependency resolution
	var comprehensiveMapper warehouse.FieldTypeMapper[SpecificBigQueryType]
	deferredMapper := warehouse.NewDeferredMapper(func() warehouse.FieldTypeMapper[SpecificBigQueryType] {
		return comprehensiveMapper
	})

	arrayMapper := &bigQueryArrayTypeMapper{SubMapper: deferredMapper}
	nestedMapper := &bigQueryNestedTypeMapper{SubMapper: deferredMapper}
	nullableMapper := &bigQueryNullableTypeMapper{SubMapper: deferredMapper}

	allMappers := []warehouse.FieldTypeMapper[SpecificBigQueryType]{
		arrayMapper,
		nestedMapper,
		nullableMapper,
	}
	allMappers = append(allMappers, baseMappers...)

	comprehensiveMapper = warehouse.NewTypeMapper(allMappers)

	return comprehensiveMapper
}
