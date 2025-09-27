package warehouse

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// ErrUnsupportedMapping represents an error when an Arrow type is not supported by a mapper
type ErrUnsupportedMapping struct {
	Type   any
	Mapper string
}

// Error implements the error interface
func (e *ErrUnsupportedMapping) Error() string {
	return fmt.Sprintf("mapper %q does not support Arrow type %q", e.Mapper, e.Type)
}

// NewUnsupportedMappingErr creates a new ErrUnsupportedArrowType
func NewUnsupportedMappingErr(theType any, mapper string) *ErrUnsupportedMapping {
	return &ErrUnsupportedMapping{
		Type:   theType,
		Mapper: mapper,
	}
}

// ErrUnsupportedWarehouseType represents an error when a warehouse type is not supported
type ErrUnsupportedWarehouseType struct {
	WarehouseType string
	Operation     string
}

// Error implements the error interface
func (e *ErrUnsupportedWarehouseType) Error() string {
	return fmt.Sprintf("warehouse type %q does not support operation %q", e.WarehouseType, e.Operation)
}

// NewUnsupportedWarehouseTypeError creates a new ErrUnsupportedWarehouseType
func NewUnsupportedWarehouseTypeError(warehouseType, operation string) *ErrUnsupportedWarehouseType {
	return &ErrUnsupportedWarehouseType{
		WarehouseType: warehouseType,
		Operation:     operation,
	}
}

// ErrTableNotFound represents an error when a table is not found
type ErrTableNotFound struct {
	TableName string
}

// Error implements the error interface
func (e *ErrTableNotFound) Error() string {
	return fmt.Sprintf("table %q not found", e.TableName)
}

// NewTableNotFoundError creates a new ErrTableNotFound
func NewTableNotFoundError(tableName string) *ErrTableNotFound {
	return &ErrTableNotFound{
		TableName: tableName,
	}
}

// ErrInvalidTableName represents an error when a table name is invalid
type ErrInvalidTableName struct {
	TableName string
	Reason    string
}

// Error implements the error interface
func (e *ErrInvalidTableName) Error() string {
	return fmt.Sprintf("invalid table name %q: %s", e.TableName, e.Reason)
}

// NewInvalidTableNameError creates a new ErrInvalidTableName
func NewInvalidTableNameError(tableName, reason string) *ErrInvalidTableName {
	return &ErrInvalidTableName{
		TableName: tableName,
		Reason:    reason,
	}
}

// ErrTypeIncompatible represents an error when a column type is incompatible with the expected type
type ErrTypeIncompatible struct {
	TableName     string
	ColumnName    string
	ExistingType  arrow.DataType
	ExpectedType  arrow.DataType
	DetailedError string
}

// Error implements the error interface
func (e *ErrTypeIncompatible) Error() string {
	if e.DetailedError != "" {
		return fmt.Sprintf("column %q in table %q has incompatible type: %s",
			e.ColumnName, e.TableName, e.DetailedError)
	}
	return fmt.Sprintf("column %q in table %q has incompatible type: existing=%q, expected=%q: %s",
		e.ColumnName, e.TableName, e.ExistingType, e.ExpectedType, e.DetailedError)
}

// NewTypeIncompatibleError creates a new ErrTypeIncompatible
func NewTypeIncompatibleError(
	tableName, columnName string,
	existingType, expectedType arrow.DataType,
) *ErrTypeIncompatible {
	return &ErrTypeIncompatible{
		TableName:    tableName,
		ColumnName:   columnName,
		ExistingType: existingType,
		ExpectedType: expectedType,
	}
}

// NewTypeIncompatibleErrorWithDetail creates a new ErrTypeIncompatible with detailed error message
func NewTypeIncompatibleErrorWithDetail(
	tableName, columnName string,
	existingType, expectedType arrow.DataType,
	detailedError string,
) *ErrTypeIncompatible {
	return &ErrTypeIncompatible{
		TableName:     tableName,
		ColumnName:    columnName,
		ExistingType:  existingType,
		ExpectedType:  expectedType,
		DetailedError: detailedError,
	}
}

// ErrMultipleTypeIncompatible represents multiple type incompatibility errors
type ErrMultipleTypeIncompatible struct {
	TableName string
	Errors    []*ErrTypeIncompatible
}

// Error implements the error interface
func (e *ErrMultipleTypeIncompatible) Error() string {
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}

	var msg = fmt.Sprintf("table %q has %d type incompatibilities:", e.TableName, len(e.Errors))
	for _, err := range e.Errors {
		msg += fmt.Sprintf("\n  - column %q: existing=%q, expected=%q (%s)",
			err.ColumnName, err.ExistingType, err.ExpectedType, err.DetailedError)
	}
	return msg
}

// NewMultipleTypeIncompatibleError creates a new ErrMultipleTypeIncompatible
func NewMultipleTypeIncompatibleError(tableName string, errors []*ErrTypeIncompatible) *ErrMultipleTypeIncompatible {
	return &ErrMultipleTypeIncompatible{
		TableName: tableName,
		Errors:    errors,
	}
}

// ErrColumnAlreadyExists represents an error when a column already exists
type ErrColumnAlreadyExists struct {
	TableName  string
	ColumnName string
}

// Error implements the error interface
func (e *ErrColumnAlreadyExists) Error() string {
	return fmt.Sprintf("column %q already exists in table %q", e.ColumnName, e.TableName)
}

// NewColumnAlreadyExistsError creates a new ErrColumnAlreadyExists
func NewColumnAlreadyExistsError(tableName, columnName string) *ErrColumnAlreadyExists {
	return &ErrColumnAlreadyExists{
		TableName:  tableName,
		ColumnName: columnName,
	}
}

// ErrTableAlreadyExists represents an error when a table already exists
type ErrTableAlreadyExists struct {
	TableName string
}

// Error implements the error interface
func (e *ErrTableAlreadyExists) Error() string {
	return fmt.Sprintf("table %q already exists", e.TableName)
}

// NewTableAlreadyExistsError creates a new ErrTableAlreadyExists error.
func NewTableAlreadyExistsError(tableName string) *ErrTableAlreadyExists {
	return &ErrTableAlreadyExists{
		TableName: tableName,
	}
}
