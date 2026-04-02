package files

import (
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/parquet-go/parquet-go"
)

const (
	parquetMapperName    = "parquet"
	parquetMetadataKeyPT = "Parquet.ParentType"
)

// SpecificParquetType represents a parquet node with formatting behavior.
type SpecificParquetType struct {
	Node       parquet.Node
	FormatFunc func(i any, m arrow.Metadata) (any, error)
}

// Format implements warehouse.SpecificWarehouseType.
func (t SpecificParquetType) Format(i any, m arrow.Metadata) (any, error) {
	return t.FormatFunc(i, m)
}

type parquetStringTypeMapper struct{}

func (m *parquetStringTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	if arrowType.ArrowDataType != arrow.BinaryTypes.String {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	return SpecificParquetType{
		Node: parquet.String(),
		FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
			s, ok := i.(string)
			if !ok {
				return nil, fmt.Errorf("expected string, got %T", i)
			}
			return s, nil
		},
	}, nil
}

func (m *parquetStringTypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetInt64TypeMapper struct{}

func (m *parquetInt64TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Int64 {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	return SpecificParquetType{
		Node: parquet.Int(64),
		FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
			v, err := toInt64(i)
			if err != nil {
				return nil, err
			}
			return v, nil
		},
	}, nil
}

func (m *parquetInt64TypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetInt32TypeMapper struct{}

func (m *parquetInt32TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Int32 {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	return SpecificParquetType{
		Node: parquet.Int(32),
		FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
			v, err := toInt32(i)
			if err != nil {
				return nil, err
			}
			return v, nil
		},
	}, nil
}

func (m *parquetInt32TypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetFloat64TypeMapper struct{}

func (m *parquetFloat64TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Float64 {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	return SpecificParquetType{
		Node: parquet.Leaf(parquet.DoubleType),
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
	}, nil
}

func (m *parquetFloat64TypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetFloat32TypeMapper struct{}

func (m *parquetFloat32TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	if arrowType.ArrowDataType != arrow.PrimitiveTypes.Float32 {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	return SpecificParquetType{
		Node: parquet.Leaf(parquet.FloatType),
		FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
			switch v := i.(type) {
			case float32:
				return v, nil
			case float64:
				if v > math.MaxFloat32 || v < -math.MaxFloat32 {
					return nil, fmt.Errorf("float64 value %v overflows float32 range", v)
				}
				return float32(v), nil
			default:
				return nil, fmt.Errorf("expected float32-compatible type, got %T", i)
			}
		},
	}, nil
}

func (m *parquetFloat32TypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetBoolTypeMapper struct{}

func (m *parquetBoolTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	if arrowType.ArrowDataType != arrow.FixedWidthTypes.Boolean {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	return SpecificParquetType{
		Node: parquet.Leaf(parquet.BooleanType),
		FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
			b, ok := i.(bool)
			if !ok {
				return nil, fmt.Errorf("expected bool, got %T", i)
			}
			return b, nil
		},
	}, nil
}

func (m *parquetBoolTypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetTimestampTypeMapper struct{}

func (m *parquetTimestampTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	supported := false
	if t, ok := arrowType.ArrowDataType.(*arrow.TimestampType); ok {
		supported = t.Unit == arrow.Second && (t.TimeZone == "" || t.TimeZone == "UTC")
	}

	if !supported {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	return SpecificParquetType{
		Node: parquet.Timestamp(parquet.Millisecond),
		FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
			switch v := i.(type) {
			case string:
				t, err := time.Parse(time.RFC3339, v)
				if err != nil {
					return nil, fmt.Errorf("parsing RFC3339 timestamp: %w", err)
				}
				return t.UTC(), nil
			case time.Time:
				return v.UTC(), nil
			case float64:
				if math.IsNaN(v) || math.IsInf(v, 0) {
					return nil, fmt.Errorf("invalid unix seconds value: %v", v)
				}
				nanos := int64(v * float64(time.Second))
				return time.Unix(0, nanos).UTC(), nil
			default:
				return nil, fmt.Errorf("expected RFC3339 string, time.Time, or unix seconds float64, got %T", i)
			}
		},
	}, nil
}

func (m *parquetTimestampTypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetDate32TypeMapper struct{}

func (m *parquetDate32TypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	if arrowType.ArrowDataType != arrow.FixedWidthTypes.Date32 {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	return SpecificParquetType{
		Node: parquet.Date(),
		FormatFunc: func(i any, _ arrow.Metadata) (any, error) {
			switch v := i.(type) {
			case string:
				t, err := time.Parse("2006-01-02", v)
				if err != nil {
					t, err = time.Parse(time.RFC3339, v)
					if err != nil {
						return nil, fmt.Errorf("parsing date32 string: %w", err)
					}
				}
				return dateOnlyUTC(t), nil
			case time.Time:
				return dateOnlyUTC(v), nil
			default:
				return nil, fmt.Errorf("expected date string or time.Time, got %T", i)
			}
		},
	}, nil
}

func (m *parquetDate32TypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetNullableTypeMapper struct {
	SubMapper warehouse.FieldTypeMapper[SpecificParquetType]
}

func (m *parquetNullableTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	if !arrowType.Nullable {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	innerArrowType := arrowType.Copy()
	innerArrowType.Nullable = false
	innerType, err := m.SubMapper.ArrowToWarehouse(innerArrowType)
	if err != nil {
		return SpecificParquetType{}, err
	}

	return SpecificParquetType{
		Node: parquet.Optional(innerType.Node),
		FormatFunc: func(i any, metadata arrow.Metadata) (any, error) {
			switch v := i.(type) {
			case nil:
				return v, nil
			default:
				return innerType.Format(i, metadata)
			}
		},
	}, nil
}

func (m *parquetNullableTypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetArrayTypeMapper struct {
	SubMapper warehouse.FieldTypeMapper[SpecificParquetType]
}

func (m *parquetArrayTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	listType, ok := arrowType.ArrowDataType.(*arrow.ListType)
	if !ok {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	elementType, err := m.SubMapper.ArrowToWarehouse(warehouse.ArrowType{
		ArrowDataType: listType.Elem(),
		Nullable:      listType.ElemField().Nullable,
		Metadata:      arrow.NewMetadata([]string{parquetMetadataKeyPT}, []string{"array"}),
	})
	if err != nil {
		return SpecificParquetType{}, err
	}

	return SpecificParquetType{
		Node: parquet.List(elementType.Node),
		FormatFunc: func(i any, metadata arrow.Metadata) (any, error) {
			if i == nil {
				return nil, fmt.Errorf("expected []any for array, got <nil>")
			}

			slice, ok := i.([]any)
			if !ok {
				return nil, fmt.Errorf("expected []any for array, got %T", i)
			}

			out := make([]any, len(slice))
			for idx, elem := range slice {
				formatted, formatErr := elementType.Format(elem, metadata)
				if formatErr != nil {
					return nil, fmt.Errorf("formatting array element at index %d: %w", idx, formatErr)
				}
				out[idx] = formatted
			}
			return out, nil
		},
	}, nil
}

func (m *parquetArrayTypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

type parquetNestedTypeMapper struct {
	SubMapper warehouse.FieldTypeMapper[SpecificParquetType]
}

func (m *parquetNestedTypeMapper) ArrowToWarehouse(arrowType warehouse.ArrowType) (SpecificParquetType, error) {
	structType, ok := arrowType.ArrowDataType.(*arrow.StructType)
	if !ok {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	if getParquetParentType(arrowType) != "array" {
		return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(arrowType.ArrowDataType, parquetMapperName)
	}

	fieldTypes := make([]SpecificParquetType, 0, structType.NumFields())
	group := make(parquet.Group, structType.NumFields())

	for _, field := range structType.Fields() {
		switch field.Type.(type) {
		case *arrow.ListType, *arrow.StructType:
			return SpecificParquetType{}, warehouse.NewUnsupportedMappingErr(field.Type, parquetMapperName)
		}

		fieldType, err := m.SubMapper.ArrowToWarehouse(warehouse.ArrowType{
			ArrowDataType: field.Type,
			Nullable:      field.Nullable,
			Metadata:      warehouse.MergeArrowMetadata(field.Metadata, parquetMetadataKeyPT, "struct"),
		})
		if err != nil {
			return SpecificParquetType{}, fmt.Errorf("mapping struct field %s: %w", field.Name, err)
		}

		group[field.Name] = fieldType.Node
		fieldTypes = append(fieldTypes, fieldType)
	}

	return SpecificParquetType{
		Node: group,
		FormatFunc: func(i any, metadata arrow.Metadata) (any, error) {
			record, ok := i.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected map[string]any for struct, got %T", i)
			}

			for idx, field := range structType.Fields() {
				value, exists := record[field.Name]
				if !exists {
					continue
				}

				formatted, formatErr := fieldTypes[idx].Format(value, metadata)
				if formatErr != nil {
					return nil, fmt.Errorf("formatting struct field %s: %w", field.Name, formatErr)
				}
				record[field.Name] = formatted
			}

			return record, nil
		},
	}, nil
}

func (m *parquetNestedTypeMapper) WarehouseToArrow(SpecificParquetType) (warehouse.ArrowType, error) {
	return warehouse.ArrowType{}, warehouse.NewUnsupportedMappingErr("parquet node", parquetMapperName)
}

// NewParquetFieldTypeMapper creates a mapper that supports parquet nodes.
func NewParquetFieldTypeMapper() warehouse.FieldTypeMapper[SpecificParquetType] {
	baseMappers := []warehouse.FieldTypeMapper[SpecificParquetType]{
		&parquetStringTypeMapper{},
		&parquetInt64TypeMapper{},
		&parquetInt32TypeMapper{},
		&parquetFloat64TypeMapper{},
		&parquetFloat32TypeMapper{},
		&parquetBoolTypeMapper{},
		&parquetTimestampTypeMapper{},
		&parquetDate32TypeMapper{},
	}

	var comprehensiveMapper warehouse.FieldTypeMapper[SpecificParquetType]
	deferredMapper := warehouse.NewDeferredMapper(func() warehouse.FieldTypeMapper[SpecificParquetType] {
		return comprehensiveMapper
	})

	allMappers := make([]warehouse.FieldTypeMapper[SpecificParquetType], 0, 3+len(baseMappers))
	allMappers = append(allMappers,
		&parquetNullableTypeMapper{SubMapper: deferredMapper},
		&parquetArrayTypeMapper{SubMapper: deferredMapper},
		&parquetNestedTypeMapper{SubMapper: deferredMapper},
	)
	allMappers = append(allMappers, baseMappers...)

	comprehensiveMapper = warehouse.NewTypeMapper(allMappers)
	return comprehensiveMapper
}

func buildParquetSchema(
	arrowSchema *arrow.Schema,
	mapper warehouse.FieldTypeMapper[SpecificParquetType],
) (*parquet.Schema, []func(any, arrow.Metadata) (any, error), error) {
	fields := arrowSchema.Fields()
	group := make(parquet.Group, len(fields))
	formatFuncs := make([]func(any, arrow.Metadata) (any, error), 0, len(fields))

	for _, field := range fields {
		parquetType, err := mapper.ArrowToWarehouse(warehouse.ArrowType{
			ArrowDataType: field.Type,
			Nullable:      field.Nullable,
			Metadata:      field.Metadata,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("mapping field %s to parquet: %w", field.Name, err)
		}

		group[field.Name] = parquetType.Node
		formatFuncs = append(formatFuncs, parquetType.FormatFunc)
	}

	return parquet.NewSchema("root", group), formatFuncs, nil
}

func getParquetParentType(arrowType warehouse.ArrowType) string {
	value, ok := warehouse.GetArrowMetadataValue(arrowType.Metadata, parquetMetadataKeyPT)
	if !ok {
		return ""
	}
	return value
}

func toInt64(i any) (int64, error) {
	switch v := i.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("invalid float64 int value: %v", v)
		}
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("float64 value %v is not an integer", v)
		}
		if v > float64(math.MaxInt64) || v < float64(math.MinInt64) {
			return 0, fmt.Errorf("float64 value %v overflows int64 range", v)
		}
		return int64(v), nil
	default:
		return 0, fmt.Errorf("expected int64-compatible type, got %T", i)
	}
}

func toInt32(i any) (int32, error) {
	v, err := toInt64(i)
	if err != nil {
		return 0, err
	}
	if v > int64(math.MaxInt32) || v < int64(math.MinInt32) {
		return 0, fmt.Errorf("int64 value %d overflows int32 range", v)
	}
	return int32(v), nil
}

func dateOnlyUTC(t time.Time) time.Time {
	year, month, day := t.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
