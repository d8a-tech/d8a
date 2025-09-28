package warehouse

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// TypeNotFoundInRegistryErr represents an error when a field type is not found in the registry
type TypeNotFoundInRegistryErr struct {
	TypeName string
	TypeKind string
}

func (e *TypeNotFoundInRegistryErr) Error() string {
	return fmt.Sprintf("%s type %s not found in registry", e.TypeKind, e.TypeName)
}

// SpecificWarehouseType defines the interface for warehouse data types
type SpecificWarehouseType interface {
	Format(i any, m arrow.Metadata) (any, error)
}

// ArrowType represents an Arrow data type with metadata and nullability information
type ArrowType struct {
	ArrowDataType arrow.DataType
	Nullable      bool
	Metadata      arrow.Metadata
}

// Copy creates a copy of the ArrowType
func (a ArrowType) Copy() ArrowType {
	return ArrowType{
		ArrowDataType: a.ArrowDataType,
		Nullable:      a.Nullable,
		Metadata:      a.Metadata,
	}
}

// FieldTypeMapper provides bidirectional conversion between Arrow types and warehouse-specific types.
// Generic interface parameterized by warehouse type (WHT) which must implement SpecificWarehouseType.
// Defined in types.go.
type FieldTypeMapper[WHT SpecificWarehouseType] interface {
	ArrowToWarehouse(arrowType ArrowType) (WHT, error)
	WarehouseToArrow(warehouseType WHT) (ArrowType, error)
}

type deferredMapper[T SpecificWarehouseType] struct {
	getMapper func() FieldTypeMapper[T]
}

// ArrowToWarehouse implements FieldTypeMapper
func (d *deferredMapper[T]) ArrowToWarehouse(arrowType ArrowType) (T, error) {
	return d.getMapper().ArrowToWarehouse(arrowType)
}

// WarehouseToArrow implements FieldTypeMapper
func (d *deferredMapper[T]) WarehouseToArrow(warehouseType T) (ArrowType, error) {
	return d.getMapper().WarehouseToArrow(warehouseType)
}

// NewDeferredMapper creates a deferred mapper for handling circular dependencies
func NewDeferredMapper[T SpecificWarehouseType](getMapper func() FieldTypeMapper[T]) FieldTypeMapper[T] {
	return &deferredMapper[T]{getMapper: getMapper}
}

// TypeMapperImpl provides type mapping functionality with multiple type mappers
type TypeMapperImpl[T SpecificWarehouseType] struct {
	Types []FieldTypeMapper[T]
}

// ArrowToWarehouse implements FieldTypeMapper
func (m *TypeMapperImpl[T]) ArrowToWarehouse(arrowType ArrowType) (T, error) {
	for _, t := range m.Types {
		whType, err := t.ArrowToWarehouse(arrowType)
		if err == nil {
			return whType, nil
		}
	}
	var zero T
	return zero, &ErrUnsupportedMapping{
		Type:   arrowType.ArrowDataType,
		Mapper: fmt.Sprintf("%T", m),
	}
}

// WarehouseToArrow implements FieldTypeMapper
func (m *TypeMapperImpl[T]) WarehouseToArrow(warehouseType T) (ArrowType, error) {
	for _, t := range m.Types {
		arrowType, err := t.WarehouseToArrow(warehouseType)
		if err == nil {
			return arrowType, nil
		}
	}
	var zero ArrowType
	return zero, &ErrUnsupportedMapping{
		Type:   warehouseType,
		Mapper: fmt.Sprintf("%T", m),
	}
}

// NewTypeMapper creates a new type mapper with the provided type mappers
func NewTypeMapper[T SpecificWarehouseType](
	types []FieldTypeMapper[T],
) FieldTypeMapper[T] {
	return &TypeMapperImpl[T]{
		Types: types,
	}
}
