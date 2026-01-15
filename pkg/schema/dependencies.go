package schema

import (
	"fmt"
)

// AssertAllCoreColumnsPresent validates that all core column interfaces have corresponding implementations.
func AssertAllCoreColumnsPresent(columns []Column, coreColumns []Interface) error {
	columnsByID := make(map[InterfaceID]Column)
	for _, column := range columns {
		columnsByID[column.Implements().ID] = column
	}

	for _, coreColumn := range coreColumns {
		if _, ok := columnsByID[coreColumn.ID]; !ok {
			return fmt.Errorf(
				"core column %s is required but not present in columns",
				coreColumn.ID,
			)
		}
	}
	return nil
}

// AssertAllDependenciesFulfilledWithCoreColumns validates that all column dependencies are satisfied
// and that all core columns are present.
func AssertAllDependenciesFulfilledWithCoreColumns(columns []Column, coreColumns []Interface) error {
	// First validate that all core columns are present
	if err := AssertAllCoreColumnsPresent(columns, coreColumns); err != nil {
		return err
	}

	// Then validate all dependencies
	return AssertAllDependenciesFulfilled(columns)
}

// AssertAllDependenciesFulfilled validates that all column dependencies are satisfied.
func AssertAllDependenciesFulfilled(
	columns []Column,
) error {
	columnsByID := make(map[InterfaceID]Column)
	for _, column := range columns {
		columnsByID[column.Implements().ID] = column
	}

	for _, column := range columns {
		for _, dependency := range column.DependsOn() {
			_, ok := columnsByID[dependency.Interface]
			if !ok {
				return fmt.Errorf(
					"column %s depends on column %s, which is not present",
					column.Implements().ID,
					dependency.Interface,
				)
			}
		}
	}
	return nil
}
