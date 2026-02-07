package clickhouse

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
)

// ClickhouseMapperName is the name of the ClickHouse type mapper
const ClickhouseMapperName = "clickhouse"

// SpecificClickhouseType represents a ClickHouse data type with its string representation and formatting function
type SpecificClickhouseType struct {
	TypeAsString         string
	ColumnModifiers      string // e.g., "DEFAULT" for nullable fields stored as NOT NULL with DEFAULT
	DefaultValue         any    // Go-level default value for this type (nil if not applicable)
	DefaultSQLExpression string // SQL expression for DEFAULT clause (e.g., "''", "0", "'1970-01-01'")
	FormatFunc           func(i any, m arrow.Metadata) (any, error)
}

// Format formats a value according to the ClickHouse type's formatting function
func (t SpecificClickhouseType) Format(i any, m arrow.Metadata) (any, error) {
	return t.FormatFunc(i, m)
}

// === PRIMITIVE TYPE MAPPERS ===

type clickhouseStringTypeMapper struct{}

func (m *clickhouseStringTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	if arrowType.ArrowDataType != arrow.BinaryTypes.String {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}
	return clickhouseString, nil
}

func (m *clickhouseStringTypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	// Handle both DDL format and system.columns format
	if warehouseType.TypeAsString != "String" &&
		warehouseType.TypeAsString != "string" &&
		warehouseType.TypeAsString != "utf8" {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.BinaryTypes.String}, nil
}

type clickhouseInt64TypeMapper struct{}

func (m *clickhouseInt64TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Int64 {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}
	return clickhouseInt64, nil
}

func (m *clickhouseInt64TypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	// Handle both DDL format and system.columns format
	if warehouseType.TypeAsString != "Int64" && warehouseType.TypeAsString != "int64" {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int64}, nil
}

type clickhouseInt32TypeMapper struct{}

func (m *clickhouseInt32TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Int32 {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}
	return clickhouseInt32, nil
}

func (m *clickhouseInt32TypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	// Handle both DDL format and system.columns format
	if warehouseType.TypeAsString != "Int32" && warehouseType.TypeAsString != "int32" {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Int32}, nil
}

type clickhouseFloat64TypeMapper struct{}

func (m *clickhouseFloat64TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Float64 {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}
	return clickhouseFloat64, nil
}

func (m *clickhouseFloat64TypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	// Handle both DDL format and system.columns format
	if warehouseType.TypeAsString != "Float64" && warehouseType.TypeAsString != "float64" {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float64}, nil
}

type clickhouseFloat32TypeMapper struct{}

func (m *clickhouseFloat32TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Float32 {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}
	return clickhouseFloat32, nil
}

func (m *clickhouseFloat32TypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	// Handle both DDL format and system.columns format
	if warehouseType.TypeAsString != "Float32" && warehouseType.TypeAsString != "float32" {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.PrimitiveTypes.Float32}, nil
}

type clickhouseTimestampTypeMapper struct{}

const timestampFormat = "2006-01-02 15:04:05.999999999"

func (m *clickhouseTimestampTypeMapper) ArrowToWarehouse(
	arrowType warehouse.ArrowType,
) (SpecificClickhouseType, error) {
	if arrowType.ArrowDataType == arrow.FixedWidthTypes.Timestamp_s {
		return SpecificClickhouseType{
			TypeAsString:         fmt.Sprintf("DateTime64(%s)", PrecisionMetadataValueSecond),
			DefaultValue:         time.Unix(0, 0),
			DefaultSQLExpression: "'1970-01-01 00:00:00'",
			FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
				switch v := i.(type) {
				case int32:
					return time.Unix(0, int64(v)).Format(timestampFormat), nil
				case int64:
					return time.Unix(0, v).Format(timestampFormat), nil
				case string:
					// Try to parse as ISO string
					t, err := time.Parse(time.RFC3339, v)
					if err != nil {
						return nil, fmt.Errorf("invalid ISO string format: %w", err)
					}
					return t.Format(timestampFormat), nil
				case time.Time:
					return v.Format(timestampFormat), nil
				default:
					return nil, fmt.Errorf("expected int32, int64, ISO string or time.Time, got %T", i)
				}
			},
		}, nil
	}
	return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
}

func (m *clickhouseTimestampTypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	// Check if this is actually a DateTime type
	if !strings.HasPrefix(warehouseType.TypeAsString, "DateTime64(") ||
		!strings.HasSuffix(warehouseType.TypeAsString, ")") {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}

	precision, err := strconv.Atoi(warehouseType.TypeAsString[len("DateTime64(") : len(warehouseType.TypeAsString)-1])
	if err != nil {
		return warehouse.ArrowType{}, fmt.Errorf("invalid precision: %s", warehouseType.TypeAsString)
	}
	if precision != 0 {
		return warehouse.ArrowType{}, fmt.Errorf("invalid precision: %s", warehouseType.TypeAsString)
	}
	return warehouse.ArrowType{
		ArrowDataType: arrow.FixedWidthTypes.Timestamp_s,
		Metadata: arrow.NewMetadata(
			[]string{PrecisionMetadataKey},
			[]string{PrecisionMetadataValueSecond},
		),
	}, nil
}

type clickhouseBoolTypeMapper struct{}

func (m *clickhouseBoolTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	if arrowType.ArrowDataType != arrow.FixedWidthTypes.Boolean {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}
	return clickhouseBool, nil
}

func (m *clickhouseBoolTypeMapper) WarehouseToArrow(warehouseType SpecificClickhouseType) (warehouse.ArrowType, error) {
	// Handle both DDL format and system.columns format
	if warehouseType.TypeAsString != "Bool" && warehouseType.TypeAsString != "bool" {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Boolean}, nil
}

type clickhouseDate32TypeMapper struct{}

func (m *clickhouseDate32TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	if arrowType.ArrowDataType != arrow.FixedWidthTypes.Date32 {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}
	return clickhouseDate32, nil
}

func (m *clickhouseDate32TypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	// Handle both DDL format and system.columns format
	if warehouseType.TypeAsString != "Date32" && warehouseType.TypeAsString != "date32" {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}
	return warehouse.ArrowType{ArrowDataType: arrow.FixedWidthTypes.Date32}, nil
}

// === COMPLEX TYPE MAPPERS ===

type clickhouseArrayTypeMapper struct {
	SubMapper warehouse.FieldTypeMapper[SpecificClickhouseType]
}

func (m *clickhouseArrayTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	listType, ok := arrowType.ArrowDataType.(*arrow.ListType)
	if !ok {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}

	// Handle List of Struct case directly (since we support this pattern)
	if structType, isStruct := listType.Elem().(*arrow.StructType); isStruct {
		return m.handleListOfStruct(structType)
	}

	// For non-struct element types, use regular array processing
	return m.handleRegularArray(listType)
}

// handleListOfStruct processes List(Struct(...)) types with validation
func (m *clickhouseArrayTypeMapper) handleListOfStruct(structType *arrow.StructType) (SpecificClickhouseType, error) {
	// Validate that struct contains only primitive types (1-level nesting limit)
	if err := m.validateStructContainsPrimitiveTypesOnly(structType); err != nil {
		return SpecificClickhouseType{}, err
	}

	// Build nested field definitions directly
	fieldDefs := make([]string, 0, len(structType.Fields()))
	fieldTypes := make([]SpecificClickhouseType, 0, len(structType.Fields()))

	for _, field := range structType.Fields() {
		fieldArrowType := warehouse.ArrowType{ArrowDataType: field.Type, Nullable: field.Nullable}
		fieldType, err := m.SubMapper.ArrowToWarehouse(fieldArrowType)
		if err != nil {
			return SpecificClickhouseType{}, fmt.Errorf("error mapping field %s: %w", field.Name, err)
		}

		fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", field.Name, fieldType.TypeAsString))
		fieldTypes = append(fieldTypes, fieldType)
	}

	nestedTypeDef := fmt.Sprintf("Nested(%s)", strings.Join(fieldDefs, ", "))

	return SpecificClickhouseType{
		TypeAsString: nestedTypeDef,
		FormatFunc: func(i any, metadata arrow.Metadata) (any, error) {
			return m.formatNestedArray(i, structType, fieldTypes, metadata)
		},
	}, nil
}

// handleRegularArray processes non-struct array types
func (m *clickhouseArrayTypeMapper) handleRegularArray(listType *arrow.ListType) (SpecificClickhouseType, error) {
	elementArrowType := warehouse.ArrowType{ArrowDataType: listType.Elem()}
	elementMappedType, err := m.SubMapper.ArrowToWarehouse(elementArrowType)
	if err != nil {
		return SpecificClickhouseType{}, err
	}

	return SpecificClickhouseType{
		TypeAsString: fmt.Sprintf("Array(%s)", elementMappedType.TypeAsString),
		FormatFunc: func(i any, metadata arrow.Metadata) (any, error) {
			slice, ok := i.([]any)
			if !ok {
				return nil, fmt.Errorf("expected []any for array, got %T", i)
			}

			result := make([]any, len(slice))
			for idx, elem := range slice {
				formatted, err := elementMappedType.Format(elem, metadata)
				if err != nil {
					return nil, fmt.Errorf("error formatting array element at index %d: %w", idx, err)
				}
				result[idx] = formatted
			}
			return result, nil
		},
	}, nil
}

// formatNestedArray formats nested array data for ClickHouse
func (m *clickhouseArrayTypeMapper) formatNestedArray(
	i any,
	structType *arrow.StructType,
	fieldTypes []SpecificClickhouseType,
	metadata arrow.Metadata,
) (any, error) {
	slice, ok := i.([]any)
	if !ok {
		return nil, fmt.Errorf("expected []any for array, got %T", i)
	}

	result := make([]map[string]any, 0, len(slice))
	for _, record := range slice {
		recordMap := make(map[string]any)
		record, ok := record.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map[string]any for nested type element, got %T", record)
		}

		// Always populate every struct field (missing or nil values will be converted to defaults)
		for idx, field := range structType.Fields() {
			var value any
			var exists bool
			if value, exists = record[field.Name]; !exists {
				value = nil // Missing field - pass nil to formatter
			}
			// If value exists but is nil, also pass nil to formatter
			// The nullability-as-default wrapper will convert nil to the correct default
			formatted, err := fieldTypes[idx].Format(value, metadata)
			if err != nil {
				return nil, fmt.Errorf("error formatting field %s: %w", field.Name, err)
			}
			recordMap[field.Name] = formatted
		}
		result = append(result, recordMap)
	}
	return result, nil
}

// validateStructContainsPrimitiveTypesOnly validates that a struct contains only primitive types
// and no nested Lists or Structs (enforces 1-level nesting limit)
func (m *clickhouseArrayTypeMapper) validateStructContainsPrimitiveTypesOnly(structType *arrow.StructType) error {
	for _, field := range structType.Fields() {
		if !m.isPrimitiveType(field.Type) {
			return warehouse.NewUnsupportedMappingErr(field.Type, ClickhouseMapperName)
		}
	}
	return nil
}

// isPrimitiveType checks if a given Arrow type is a primitive type supported by ClickHouse
func (m *clickhouseArrayTypeMapper) isPrimitiveType(dataType arrow.DataType) bool {
	switch dataType {
	case arrow.BinaryTypes.String,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
		arrow.FixedWidthTypes.Boolean,
		arrow.FixedWidthTypes.Timestamp_s,
		arrow.FixedWidthTypes.Date32:
		return true
	default:
		return false
	}
}

func (m *clickhouseArrayTypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	// Check if it's a Nested type (which represents List of Struct in ClickHouse)
	if isNestedType(warehouseType.TypeAsString) {
		// Parse the nested type definition to get the struct fields
		parser := &clickhouseParser{}
		fields := parser.parseNestedFields(warehouseType.TypeAsString)

		// Convert each field to Arrow
		arrowFields := make([]arrow.Field, 0, len(fields))
		for _, field := range fields {
			fieldType := SpecificClickhouseType{TypeAsString: field.TypeName}
			arrowType, err := m.SubMapper.WarehouseToArrow(fieldType)
			if err != nil {
				return warehouse.ArrowType{}, fmt.Errorf("error mapping field %s: %w", field.Name, err)
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name:     field.Name,
				Type:     arrowType.ArrowDataType,
				Nullable: true,
			})
		}

		return warehouse.ArrowType{
			ArrowDataType: arrow.ListOf(arrow.StructOf(arrowFields...)),
		}, nil
	}

	// Check if it's an Array type by parsing the TypeAsString
	if !isArrayType(warehouseType.TypeAsString) {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}

	// Extract element type from Array(ElementType)
	elementTypeStr := extractArrayElementType(warehouseType.TypeAsString)
	elementType := SpecificClickhouseType{TypeAsString: elementTypeStr}

	// Map the element type back to Arrow
	elementArrowType, err := m.SubMapper.WarehouseToArrow(elementType)
	if err != nil {
		return warehouse.ArrowType{}, err
	}

	return warehouse.ArrowType{
		ArrowDataType: arrow.ListOf(elementArrowType.ArrowDataType),
	}, nil
}

type clickhouseNestedTypeMapper struct {
	parser    *clickhouseParser
	SubMapper warehouse.FieldTypeMapper[SpecificClickhouseType]
}

func (m *clickhouseNestedTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	structType, ok := arrowType.ArrowDataType.(*arrow.StructType)
	if !ok {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}

	// Validate that all struct fields contain only primitive types (1-level nesting limit)
	for _, field := range structType.Fields() {
		if !m.isPrimitiveType(field.Type) {
			return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(field.Type, ClickhouseMapperName)
		}
	}

	// Build nested field definitions
	fieldDefs := make([]string, 0, len(structType.Fields()))
	fieldTypes := make([]SpecificClickhouseType, 0, len(structType.Fields()))

	for _, field := range structType.Fields() {
		fieldArrowType := warehouse.ArrowType{ArrowDataType: field.Type}
		fieldType, err := m.SubMapper.ArrowToWarehouse(fieldArrowType)
		if err != nil {
			return SpecificClickhouseType{}, fmt.Errorf("error mapping field %s: %w", field.Name, err)
		}

		fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", field.Name, fieldType.TypeAsString))
		fieldTypes = append(fieldTypes, fieldType)
	}

	nestedTypeDef := fmt.Sprintf("Nested(%s)", strings.Join(fieldDefs, ", "))

	return SpecificClickhouseType{
		TypeAsString: nestedTypeDef,
		FormatFunc: func(i any, metadata arrow.Metadata) (any, error) {
			iAsCollection, ok := i.([]any)
			if !ok {
				iAsCollection = []any{i}
			}
			result := make([]map[string]any, 0, len(iAsCollection))
			for _, record := range iAsCollection {
				recordMap := make(map[string]any)
				record, ok := record.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("expected map[string]any for nested type, got %T", i)
				}

				// Always populate every struct field (missing or nil values will be converted to defaults)
				for idx, field := range structType.Fields() {
					var value any
					var exists bool
					if value, exists = record[field.Name]; !exists {
						value = nil // Missing field - pass nil to formatter
					}
					// If value exists but is nil, also pass nil to formatter
					// The nullability-as-default wrapper will convert nil to the correct default
					formatted, err := fieldTypes[idx].Format(value, metadata)
					if err != nil {
						return nil, fmt.Errorf("error formatting field %s: %w", field.Name, err)
					}
					recordMap[field.Name] = formatted
				}
				result = append(result, recordMap)
			}
			return result, nil
		},
	}, nil
}

// isPrimitiveType checks if a given Arrow type is a primitive type supported by ClickHouse
func (m *clickhouseNestedTypeMapper) isPrimitiveType(dataType arrow.DataType) bool {
	switch dataType {
	case arrow.BinaryTypes.String,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
		arrow.FixedWidthTypes.Boolean,
		arrow.FixedWidthTypes.Timestamp_s,
		arrow.FixedWidthTypes.Date32:
		return true
	default:
		return false
	}
}

func (m *clickhouseNestedTypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	if !isNestedType(warehouseType.TypeAsString) {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}

	// Parse the nested type definition
	fields := m.parser.parseNestedFields(warehouseType.TypeAsString)

	// Pre-allocate slices with known capacity
	fieldDefs := make([]string, 0, len(fields))
	fieldTypes := make([]SpecificClickhouseType, 0, len(fields))

	for _, field := range fields {
		fieldDefs = append(fieldDefs, field.Name)
		fieldTypes = append(fieldTypes, SpecificClickhouseType{TypeAsString: field.TypeName})
	}

	// Map each field type to Arrow
	arrowFields := make([]arrow.Field, 0, len(fieldTypes))
	for i, fieldType := range fieldTypes {
		arrowType, err := m.SubMapper.WarehouseToArrow(fieldType)
		if err != nil {
			return warehouse.ArrowType{}, fmt.Errorf("error mapping field %s: %w", fieldDefs[i], err)
		}
		arrowFields = append(arrowFields, arrow.Field{
			Name:     fieldDefs[i],
			Type:     arrowType.ArrowDataType,
			Nullable: true,
		})
	}

	return warehouse.ArrowType{
		ArrowDataType: arrow.StructOf(arrowFields...),
	}, nil
}

func newClickhouseNestedTypeMapper(
	subMapper warehouse.FieldTypeMapper[SpecificClickhouseType],
) warehouse.FieldTypeMapper[SpecificClickhouseType] {
	return &clickhouseNestedTypeMapper{
		parser:    &clickhouseParser{},
		SubMapper: subMapper,
	}
}

// clickhouseNullabilityAsDefaultMapper converts nullable Arrow fields to NOT NULL with DEFAULT
// This avoids Nullable storage overhead in ClickHouse while preserving semantic nullability
type clickhouseNullabilityAsDefaultMapper struct {
	SubMapper warehouse.FieldTypeMapper[SpecificClickhouseType]
}

func (m *clickhouseNullabilityAsDefaultMapper) ArrowToWarehouse(
	arrowType warehouse.ArrowType,
) (SpecificClickhouseType, error) {
	// Only handle nullable scalar types (not arrays/structs)
	if !arrowType.Nullable {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}

	// Reject ListType and StructType - they should not use this mapper
	if _, isListType := arrowType.ArrowDataType.(*arrow.ListType); isListType {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}
	if _, isStructType := arrowType.ArrowDataType.(*arrow.StructType); isStructType {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}

	// Create inner type with Nullable=false
	innerType := arrowType.Copy()
	innerType.Nullable = false

	// Map the inner type
	innerMappedType, err := m.SubMapper.ArrowToWarehouse(innerType)
	if err != nil {
		return SpecificClickhouseType{}, err
	}

	// Create nil-safe wrapper that converts nil to type-specific defaults
	nilSafeFormatFunc := func(i any, metadata arrow.Metadata) (any, error) {
		if i == nil {
			// Convert nil to type-specific default based on Arrow type
			defaultValue := m.getDefaultValueForType(arrowType.ArrowDataType)
			return innerMappedType.Format(defaultValue, metadata)
		}
		return innerMappedType.Format(i, metadata)
	}

	return SpecificClickhouseType{
		TypeAsString:         innerMappedType.TypeAsString,
		ColumnModifiers:      "DEFAULT",
		DefaultValue:         innerMappedType.DefaultValue,
		DefaultSQLExpression: innerMappedType.DefaultSQLExpression,
		FormatFunc:           nilSafeFormatFunc,
	}, nil
}

func (m *clickhouseNullabilityAsDefaultMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	// This mapper only handles ArrowToWarehouse direction
	// WarehouseToArrow is handled by the regular mappers
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
}

// getDefaultValueForType returns the Go-level default value for a given Arrow type
// by using the type mapper to get the mapped type's DefaultValue
func (m *clickhouseNullabilityAsDefaultMapper) getDefaultValueForType(dataType arrow.DataType) any {
	arrowType := warehouse.ArrowType{ArrowDataType: dataType, Nullable: false}
	chType, err := m.SubMapper.ArrowToWarehouse(arrowType)
	if err != nil {
		return nil
	}
	return chType.DefaultValue
}

type clickhouseNullableTypeMapper struct {
	SubMapper warehouse.FieldTypeMapper[SpecificClickhouseType]
}

func (m *clickhouseNullableTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	// ClickHouse doesn't support nullable arrays - reject array types
	if _, isListType := arrowType.ArrowDataType.(*arrow.ListType); isListType {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}

	// For nullable types, we just pass through the inner type and mark it as nullable
	// In real ClickHouse, this would be handled by the field's nullable property
	if !arrowType.Nullable {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}

	newInstance := arrowType.Copy()
	newInstance.Nullable = false
	innerType, err := m.SubMapper.ArrowToWarehouse(newInstance)
	if err != nil {
		return SpecificClickhouseType{}, err
	}

	return SpecificClickhouseType{
		TypeAsString: fmt.Sprintf("Nullable(%s)", innerType.TypeAsString),
		FormatFunc: func(i any, metadata arrow.Metadata) (any, error) {
			if i == nil {
				return nil, nil //nolint:nilnil // nil is a valid value for Nullable type in ClickHouse
			}
			return innerType.Format(i, metadata)
		},
	}, nil
}

func (m *clickhouseNullableTypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	if !isNullableType(warehouseType.TypeAsString) {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}

	// Extract inner type from Nullable(InnerType)
	innerTypeStr := extractNullableInnerType(warehouseType.TypeAsString)

	// ClickHouse doesn't support nullable arrays or nested types - reject them
	if isArrayType(innerTypeStr) || isNestedType(innerTypeStr) {
		return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr(warehouseType, ClickhouseMapperName)
	}

	innerType := SpecificClickhouseType{TypeAsString: innerTypeStr}

	// Map the inner type back to Arrow
	innerArrowType, err := m.SubMapper.WarehouseToArrow(innerType)
	if err != nil {
		return warehouse.ArrowType{}, err
	}
	innerArrowType.Nullable = true
	return innerArrowType, nil
}

var clickhouseBool = SpecificClickhouseType{
	TypeAsString:         "Bool",
	DefaultValue:         false,
	DefaultSQLExpression: "0",
	FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
		return i, nil
	},
}

var clickhouseString = SpecificClickhouseType{
	TypeAsString:         "String",
	DefaultValue:         "",
	DefaultSQLExpression: "''",
	FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
		iAsStr, ok := i.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", i)
		}
		return iAsStr, nil
	},
}

var clickhouseInt64 = SpecificClickhouseType{
	TypeAsString:         "Int64",
	DefaultValue:         int64(0),
	DefaultSQLExpression: "0",
	FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
		switch v := i.(type) {
		case int64:
			return v, nil
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		default:
			return nil, fmt.Errorf("expected int64-compatible type, got %T", i)
		}
	},
}

var clickhouseInt32 = SpecificClickhouseType{
	TypeAsString:         "Int32",
	DefaultValue:         int32(0),
	DefaultSQLExpression: "0",
	FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
		switch v := i.(type) {
		case int32:
			return v, nil
		case int:
			return int32(v), nil
		case int64:
			return int32(v), nil
		default:
			return nil, fmt.Errorf("expected int32-compatible type, got %T", i)
		}
	},
}

var clickhouseFloat64 = SpecificClickhouseType{
	TypeAsString:         "Float64",
	DefaultValue:         float64(0),
	DefaultSQLExpression: "0",
	FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
		switch v := i.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		default:
			return nil, fmt.Errorf("expected float64-compatible type, got %T", i)
		}
	},
}

var clickhouseFloat32 = SpecificClickhouseType{
	TypeAsString:         "Float32",
	DefaultValue:         float32(0),
	DefaultSQLExpression: "0",
	FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
		switch v := i.(type) {
		case float32:
			return v, nil
		case float64:
			return float32(v), nil
		default:
			return nil, fmt.Errorf("expected float32-compatible type, got %T", i)
		}
	},
}

var clickhouseDate32 = SpecificClickhouseType{
	TypeAsString:         "Date32",
	DefaultValue:         time.Unix(0, 0),
	DefaultSQLExpression: "'1970-01-01'",
	FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
		switch v := i.(type) {
		case time.Time:
			// Format as YYYY-MM-DD for ClickHouse Date32 type
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
	},
}

func anyType(asString string) SpecificClickhouseType {
	return SpecificClickhouseType{
		TypeAsString: asString,
		FormatFunc: func(_ any, _ arrow.Metadata) (any, error) {
			return nil, fmt.Errorf("this type does not support formatting: %s", asString)
		},
	}
}

// restrictedNestedTypeMapper wraps the nested type mapper to prevent direct struct access
// while still allowing structs to be used within arrays (List of Struct pattern)
type restrictedNestedTypeMapper struct {
	actualMapper warehouse.FieldTypeMapper[SpecificClickhouseType]
}

func (r *restrictedNestedTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificClickhouseType, error) {
	// Reject direct struct types - ClickHouse doesn't support non-repeated nested fields
	if _, isStruct := arrowType.ArrowDataType.(*arrow.StructType); isStruct {
		return SpecificClickhouseType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, ClickhouseMapperName)
	}
	return r.actualMapper.ArrowToWarehouse(arrowType)
}

func (r *restrictedNestedTypeMapper) WarehouseToArrow(
	warehouseType SpecificClickhouseType,
) (warehouse.ArrowType, error) {
	return r.actualMapper.WarehouseToArrow(warehouseType)
}

// NewFieldTypeMapper creates a mapper that supports all ClickHouse types with proper circular dependency handling
func NewFieldTypeMapper() warehouse.FieldTypeMapper[SpecificClickhouseType] {
	// Base primitive mappers
	baseMappers := []warehouse.FieldTypeMapper[SpecificClickhouseType]{
		&clickhouseStringTypeMapper{},
		&clickhouseInt64TypeMapper{},
		&clickhouseInt32TypeMapper{},
		&clickhouseFloat64TypeMapper{},
		&clickhouseFloat32TypeMapper{},
		&clickhouseTimestampTypeMapper{},
		&clickhouseBoolTypeMapper{},
		&clickhouseDate32TypeMapper{},
	}

	// deferred mapper for circular dependency resolution
	var comprehensiveMapper warehouse.FieldTypeMapper[SpecificClickhouseType]
	deferredMapper := warehouse.NewDeferredMapper(func() warehouse.FieldTypeMapper[SpecificClickhouseType] {
		return comprehensiveMapper
	})

	arrayMapper := &clickhouseArrayTypeMapper{SubMapper: deferredMapper}

	// Create restricted nested mapper that rejects direct struct access
	actualNestedMapper := newClickhouseNestedTypeMapper(deferredMapper)
	restrictedNestedMapper := &restrictedNestedTypeMapper{
		actualMapper: actualNestedMapper,
	}

	// nullability-as-default mapper must come BEFORE nullable mapper
	// so nullable scalars are converted to DEFAULT instead of Nullable(...)
	nullabilityAsDefaultMapper := &clickhouseNullabilityAsDefaultMapper{SubMapper: deferredMapper}
	nullableMapper := &clickhouseNullableTypeMapper{SubMapper: deferredMapper}

	complexMappers := []warehouse.FieldTypeMapper[SpecificClickhouseType]{
		arrayMapper,
		restrictedNestedMapper,
		nullabilityAsDefaultMapper, // Must be before nullableMapper
		nullableMapper,
	}

	comprehensiveMapper = warehouse.NewTypeMapper(append(complexMappers, baseMappers...))

	return comprehensiveMapper
}
