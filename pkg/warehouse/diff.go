package warehouse

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// TypeComparisonResult represents the result of comparing two Arrow data types
type TypeComparisonResult struct {
	Equal        bool
	ErrorMessage string
	TypePath     string
}

// TypeCompatibilityRule defines a function that can determine if two types should be considered compatible.
// Returns:
//   - compatible: whether the types should be considered compatible (only meaningful if handled=true)
//   - handled: whether this rule applies to and processed the given type pair. If false, other rules
//     or default comparison logic will be used. If true, the compatibility decision is final.
type TypeCompatibilityRule func(expected, actual arrow.DataType) (compatible bool, handled bool)

// TypeComparer holds comparison configuration and custom rules
type TypeComparer struct {
	compatibilityRules []TypeCompatibilityRule
}

// NewTypeComparer creates a new comparer with the specified compatibility rules
func NewTypeComparer(rules ...TypeCompatibilityRule) *TypeComparer {
	return &TypeComparer{
		compatibilityRules: rules,
	}
}

// Compare performs the comparison with custom rules applied
func (tc *TypeComparer) Compare(expected, actual arrow.DataType, typePath string) TypeComparisonResult {
	return tc.compareArrowTypesRecursive(expected, actual, typePath, typePath)
}

// CompareArrowTypes performs deep comparison of two Arrow data types and returns detailed comparison results.
// This maintains backward compatibility by using the default TypeComparer.
func CompareArrowTypes(expected, actual arrow.DataType, typePath string) TypeComparisonResult {
	comparer := NewTypeComparer()
	return comparer.Compare(expected, actual, typePath)
}

// createTypeComparisonResult creates a TypeComparisonResult with consistent formatting
func createTypeComparisonResult(equal bool, _, originalPath, errorMsg string) TypeComparisonResult {
	return TypeComparisonResult{
		Equal:        equal,
		ErrorMessage: errorMsg,
		TypePath:     originalPath,
	}
}

// compareTypeIDs performs the initial type ID comparison, checking compatibility rules first
func (tc *TypeComparer) compareTypeIDs(
	expected, actual arrow.DataType, currentPath, originalPath string,
) (TypeComparisonResult, bool) {
	// First check custom compatibility rules
	for _, rule := range tc.compatibilityRules {
		if compatible, handled := rule(expected, actual); handled {
			if compatible {
				return createTypeComparisonResult(true, currentPath, originalPath, ""), true
			}
			// If rule handled but not compatible, continue with normal comparison
			break
		}
	}

	expectedID := expected.ID()
	actualID := actual.ID()

	if expectedID != actualID {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: type IDs differ - expected: %s, actual: %s", currentPath, expectedID, actualID),
		), false
	}
	return TypeComparisonResult{}, true
}

// compareListTypes compares two Arrow list types
func (tc *TypeComparer) compareListTypes(
	expected, actual arrow.DataType, currentPath, originalPath string,
) TypeComparisonResult {
	expectedList, ok := expected.(*arrow.ListType)
	if !ok {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: failed to cast expected type to ListType", currentPath),
		)
	}
	actualList, ok := actual.(*arrow.ListType)
	if !ok {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: failed to cast actual type to ListType", currentPath),
		)
	}

	// Compare element types recursively
	return tc.compareArrowTypesRecursive(
		expectedList.Elem(),
		actualList.Elem(),
		fmt.Sprintf("%s[LIST_ELEMENT]", currentPath),
		originalPath,
	)
}

// compareStructTypes compares two Arrow struct types
func (tc *TypeComparer) compareStructTypes(
	expected, actual arrow.DataType, currentPath, originalPath string,
) TypeComparisonResult {
	expectedStruct, ok := expected.(*arrow.StructType)
	if !ok {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: failed to cast expected type to StructType", currentPath),
		)
	}
	actualStruct, ok := actual.(*arrow.StructType)
	if !ok {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: failed to cast actual type to StructType", currentPath),
		)
	}
	expectedStructFields := expectedStruct.Fields()
	actualStructFields := actualStruct.Fields()
	if len(expectedStructFields) != len(actualStructFields) {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: struct field counts differ - expected: %d, actual: %d",
				currentPath, len(expectedStructFields), len(actualStructFields)),
		)
	}
	for i, expectedStructField := range expectedStructFields {
		actualStructField := actualStructFields[i]

		// Compare field names
		if expectedStructField.Name != actualStructField.Name {
			return createTypeComparisonResult(
				false,
				currentPath,
				originalPath,
				fmt.Sprintf("%s.STRUCT[%d]: field names differ - expected: %s, actual: %s",
					currentPath, i, expectedStructField.Name, actualStructField.Name),
			)
		}
		// Compare nullability
		if expectedStructField.Nullable != actualStructField.Nullable {
			return createTypeComparisonResult(
				false,
				currentPath,
				originalPath,
				fmt.Sprintf("%s.STRUCT[%d].%s: nullability differs - expected: %t, actual: %t",
					currentPath, i, expectedStructField.Name, expectedStructField.Nullable, actualStructField.Nullable),
			)
		}

		// Compare field types recursively
		fieldResult := tc.compareArrowTypesRecursive(
			expectedStructField.Type,
			actualStructField.Type,
			fmt.Sprintf("%s.STRUCT[%d].%s", currentPath, i, expectedStructField.Name),
			originalPath,
		)
		if !fieldResult.Equal {
			return fieldResult
		}
	}

	return createTypeComparisonResult(true, currentPath, originalPath, "")
}

// compareTimestampTypes compares two Arrow timestamp types
func (tc *TypeComparer) compareTimestampTypes(
	expected, actual arrow.DataType, currentPath, originalPath string,
) TypeComparisonResult {
	expectedTimestamp, ok := expected.(*arrow.TimestampType)
	if !ok {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: failed to cast expected type to TimestampType", currentPath),
		)
	}
	actualTimestamp, ok := actual.(*arrow.TimestampType)
	if !ok {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: failed to cast actual type to TimestampType", currentPath),
		)
	}

	if expectedTimestamp.Unit != actualTimestamp.Unit {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: timestamp units differ - expected: %s, actual: %s",
				currentPath, expectedTimestamp.Unit, actualTimestamp.Unit),
		)
	}

	if expectedTimestamp.TimeZone != actualTimestamp.TimeZone {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: timestamp timezones differ - expected: %s, actual: %s",
				currentPath, expectedTimestamp.TimeZone, actualTimestamp.TimeZone),
		)
	}

	return createTypeComparisonResult(true, currentPath, originalPath, "")
}

// comparePrimitiveTypes compares two primitive Arrow types
func (tc *TypeComparer) comparePrimitiveTypes(
	expected, actual arrow.DataType, currentPath, originalPath string,
) TypeComparisonResult {
	if expected.String() != actual.String() {
		return createTypeComparisonResult(
			false,
			currentPath,
			originalPath,
			fmt.Sprintf("%s: type string representations differ - expected: %s, actual: %s",
				currentPath, expected.String(), actual.String()),
		)
	}
	return createTypeComparisonResult(true, currentPath, originalPath, "")
}

func (tc *TypeComparer) compareArrowTypesRecursive(
	expected, actual arrow.DataType, currentPath, originalPath string,
) TypeComparisonResult {
	// Compare type IDs first (includes compatibility rules)
	if result, ok := tc.compareTypeIDs(expected, actual, currentPath, originalPath); !ok {
		return result
	} else if result.Equal {
		// If compatibility rule determined they are compatible, return immediately
		return result
	}

	// Handle specific type comparisons based on type ID
	switch expected.ID() {
	case arrow.LIST:
		return tc.compareListTypes(expected, actual, currentPath, originalPath)
	case arrow.STRUCT:
		return tc.compareStructTypes(expected, actual, currentPath, originalPath)
	case arrow.TIMESTAMP:
		return tc.compareTimestampTypes(expected, actual, currentPath, originalPath)
	default:
		return tc.comparePrimitiveTypes(expected, actual, currentPath, originalPath)
	}
}

// FieldCompatibilityChecker defines the interface for checking field compatibility
type FieldCompatibilityChecker interface {
	AreFieldsCompatible(existing, input *arrow.Field) (bool, error)
}

// FindMissingColumns compares existing table fields with input schema and returns missing fields
// or type incompatibility errors. This is the common logic extracted from both BigQuery and ClickHouse drivers.
func FindMissingColumns(
	tableName string,
	existingFields map[string]*arrow.Field,
	inputSchema *arrow.Schema,
	compatibilityChecker FieldCompatibilityChecker,
) ([]*arrow.Field, error) {
	var missingFields []*arrow.Field
	var typeErrors []*ErrTypeIncompatible

	for _, inputField := range inputSchema.Fields() {
		existingField, exists := existingFields[inputField.Name]
		if !exists {
			// Column doesn't exist, it's missing
			missingFields = append(missingFields, &inputField)
		} else {
			compatible, err := compatibilityChecker.AreFieldsCompatible(existingField, &inputField)
			if !compatible {
				// Column exists but types are incompatible
				// Use the detailed error message if available, otherwise create a generic one
				var detailedError string
				if err != nil {
					detailedError = err.Error()
				}
				typeErrors = append(typeErrors, NewTypeIncompatibleErrorWithDetail(
					tableName, inputField.Name, existingField.Type, inputField.Type, detailedError))
			}
		}
	}

	// If there are type errors, return them all at once
	if len(typeErrors) > 0 {
		return nil, NewMultipleTypeIncompatibleError(tableName, typeErrors)
	}

	return missingFields, nil
}
