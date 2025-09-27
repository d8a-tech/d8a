package schema

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
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
			dependentColumn, ok := columnsByID[dependency.Interface]
			if !ok {
				return fmt.Errorf(
					"column %s depends on column %s, which is not present",
					column.Implements().ID,
					dependency.Interface,
				)
			}

			// Parse column version and dependency constraints
			columnVersion, err := semver.NewVersion(string(dependentColumn.Implements().Version))
			if err != nil {
				return fmt.Errorf(
					"column %s has invalid version format %s: %w",
					dependency.Interface,
					dependentColumn.Implements().Version,
					err,
				)
			}

			// Check GreaterOrEqualTo constraint if specified
			if dependency.GreaterOrEqualTo != "" {
				constraint, err := semver.NewConstraint(">=" + string(dependency.GreaterOrEqualTo))
				if err != nil {
					return fmt.Errorf("invalid GreaterOrEqualTo version constraint %s: %w", dependency.GreaterOrEqualTo, err)
				}
				if !constraint.Check(columnVersion) {
					return fmt.Errorf("column %s depends on column %s version >=%s, but found version %s",
						column.Implements().ID, dependency.Interface, dependency.GreaterOrEqualTo, dependentColumn.Implements().Version)
				}
			}

			// Check LessThan constraint if specified
			if dependency.LessThan != "" {
				constraint, err := semver.NewConstraint("<" + string(dependency.LessThan))
				if err != nil {
					return fmt.Errorf("invalid LessThan version constraint %s: %w", dependency.LessThan, err)
				}
				if !constraint.Check(columnVersion) {
					return fmt.Errorf("column %s depends on column %s version <%s, but found version %s",
						column.Implements().ID, dependency.Interface, dependency.LessThan, dependentColumn.Implements().Version)
				}
			}
		}
	}
	return nil
}
