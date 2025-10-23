package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

// mockColumn implements the Column interface for testing
type mockColumn struct {
	id        InterfaceID
	version   Version
	field     *arrow.Field
	dependsOn []DependsOnEntry
}

func (m *mockColumn) Docs() Documentation {
	return Documentation{
		ColumnName:  "Mock column",
		Description: "Mock column description",
	}
}

func (m *mockColumn) Implements() Interface {
	return Interface{
		ID:      m.id,
		Version: m.version,
		Field:   m.field,
	}
}
func (m *mockColumn) DependsOn() []DependsOnEntry { return m.dependsOn }

func TestAssertAllCoreColumnsPresent(t *testing.T) {
	tests := []struct {
		name        string
		columns     []Column
		coreColumns []Interface
		expectedErr string
	}{
		{
			name: "all core columns present - should pass",
			columns: []Column{
				&mockColumn{id: "core.d8a.tech/events/id", version: "1.0.0"},
				&mockColumn{id: "core.d8a.tech/events/type", version: "1.0.0"},
				&mockColumn{id: "other.column", version: "1.0.0"},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id", Version: "1.0.0"},
				{ID: "core.d8a.tech/events/type", Version: "1.0.0"},
			},
			expectedErr: "",
		},
		{
			name: "missing core column - should fail",
			columns: []Column{
				&mockColumn{id: "core.d8a.tech/events/id", version: "1.0.0"},
				&mockColumn{id: "other.column", version: "1.0.0"},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id", Version: "1.0.0"},
				{ID: "core.d8a.tech/events/type", Version: "1.0.0"},
			},
			expectedErr: "core column core.d8a.tech/events/type is required but not present in columns",
		},
		{
			name: "no core columns required - should pass",
			columns: []Column{
				&mockColumn{id: "some.column", version: "1.0.0"},
			},
			coreColumns: []Interface{},
			expectedErr: "",
		},
		{
			name:    "empty columns with core columns required - should fail",
			columns: []Column{},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id", Version: "1.0.0"},
			},
			expectedErr: "core column core.d8a.tech/events/id is required but not present in columns",
		},
		{
			name: "multiple missing core columns - should fail on first",
			columns: []Column{
				&mockColumn{id: "other.column", version: "1.0.0"},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id", Version: "1.0.0"},
				{ID: "core.d8a.tech/events/type", Version: "1.0.0"},
			},
			expectedErr: "core column core.d8a.tech/events/id is required but not present in columns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			err := AssertAllCoreColumnsPresent(tt.columns, tt.coreColumns)

			// then
			if tt.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestAssertAllDependenciesFulfilledWithCoreColumns(t *testing.T) {
	tests := []struct {
		name        string
		columns     []Column
		coreColumns []Interface
		expectedErr string
	}{
		{
			name: "all core columns present and dependencies satisfied - should pass",
			columns: []Column{
				&mockColumn{id: "core.d8a.tech/events/id", version: "1.0.0"},
				&mockColumn{id: "core.d8a.tech/events/type", version: "1.0.0"},
				&mockColumn{
					id:      "dependent.column",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "core.d8a.tech/events/id"},
					},
				},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id", Version: "1.0.0"},
				{ID: "core.d8a.tech/events/type", Version: "1.0.0"},
			},
			expectedErr: "",
		},
		{
			name: "missing core column - should fail before dependency check",
			columns: []Column{
				&mockColumn{id: "core.d8a.tech/events/id", version: "1.0.0"},
				&mockColumn{
					id:      "dependent.column",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "core.d8a.tech/events/id"},
					},
				},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id", Version: "1.0.0"},
				{ID: "core.d8a.tech/events/type", Version: "1.0.0"},
			},
			expectedErr: "core column core.d8a.tech/events/type is required but not present in columns",
		},
		{
			name: "core columns present but dependency missing - should fail on dependency",
			columns: []Column{
				&mockColumn{id: "core.d8a.tech/events/id", version: "1.0.0"},
				&mockColumn{id: "core.d8a.tech/events/type", version: "1.0.0"},
				&mockColumn{
					id:      "dependent.column",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "missing.dependency"},
					},
				},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id", Version: "1.0.0"},
				{ID: "core.d8a.tech/events/type", Version: "1.0.0"},
			},
			expectedErr: "column dependent.column depends on column missing.dependency, which is not present",
		},
		{
			name: "no core columns required - should validate dependencies only",
			columns: []Column{
				&mockColumn{id: "col1", version: "1.0.0"},
				&mockColumn{
					id:      "col2",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1"},
					},
				},
			},
			coreColumns: []Interface{},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			err := AssertAllDependenciesFulfilledWithCoreColumns(tt.columns, tt.coreColumns)

			// then
			if tt.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestRegistry_AssertAllDependenciesFulfilled(t *testing.T) {
	tests := []struct {
		name        string
		columns     []Column
		expectedErr string
	}{
		{
			name: "no dependencies - should pass",
			columns: []Column{
				&mockColumn{id: "col1", version: "1.0.0"},
				&mockColumn{id: "col2", version: "2.0.0"},
			},
			expectedErr: "",
		},
		{
			name: "simple dependency satisfied - should pass",
			columns: []Column{
				&mockColumn{id: "col1", version: "1.0.0"},
				&mockColumn{
					id:      "col2",
					version: "2.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1"},
					},
				},
			},
			expectedErr: "",
		},
		{
			name: "missing dependency - should fail",
			columns: []Column{
				&mockColumn{
					id:      "col1",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "missing_col"},
					},
				},
			},
			expectedErr: "column col1 depends on column missing_col, which is not present",
		},
		{
			name: "GreaterOrEqualTo constraint satisfied - should pass",
			columns: []Column{
				&mockColumn{id: "col1", version: "2.5.0"},
				&mockColumn{
					id:      "col2",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1", GreaterOrEqualTo: "2.0.0"},
					},
				},
			},
			expectedErr: "",
		},
		{
			name: "GreaterOrEqualTo constraint not satisfied - should fail",
			columns: []Column{
				&mockColumn{id: "col1", version: "1.5.0"},
				&mockColumn{
					id:      "col2",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1", GreaterOrEqualTo: "2.0.0"},
					},
				},
			},
			expectedErr: "column col2 depends on column col1 version >=2.0.0, but found version 1.5.0",
		},
		{
			name: "LessThan constraint satisfied - should pass",
			columns: []Column{
				&mockColumn{id: "col1", version: "1.5.0"},
				&mockColumn{
					id:      "col2",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1", LessThan: "2.0.0"},
					},
				},
			},
			expectedErr: "",
		},
		{
			name: "LessThan constraint not satisfied - should fail",
			columns: []Column{
				&mockColumn{id: "col1", version: "2.5.0"},
				&mockColumn{
					id:      "col2",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1", LessThan: "2.0.0"},
					},
				},
			},
			expectedErr: "column col2 depends on column col1 version <2.0.0, but found version 2.5.0",
		},
		{
			name: "both constraints satisfied - should pass",
			columns: []Column{
				&mockColumn{id: "col1", version: "1.5.0"},
				&mockColumn{
					id:      "col2",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1", GreaterOrEqualTo: "1.0.0", LessThan: "2.0.0"},
					},
				},
			},
			expectedErr: "",
		},
		{
			name: "invalid dependent column version format - should fail",
			columns: []Column{
				&mockColumn{id: "col1", version: "invalid-version"},
				&mockColumn{
					id:      "col2",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1", GreaterOrEqualTo: "1.0.0"},
					},
				},
			},
			expectedErr: "column col1 has invalid version format invalid-version:",
		},
		{
			name: "invalid GreaterOrEqualTo constraint format - should fail",
			columns: []Column{
				&mockColumn{id: "col1", version: "1.0.0"},
				&mockColumn{
					id:      "col2",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1", GreaterOrEqualTo: "invalid"},
					},
				},
			},
			expectedErr: "invalid GreaterOrEqualTo version constraint invalid:",
		},
		{
			name: "invalid LessThan constraint format - should fail",
			columns: []Column{
				&mockColumn{id: "col1", version: "1.0.0"},
				&mockColumn{
					id:      "col2",
					version: "1.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1", LessThan: "invalid"},
					},
				},
			},
			expectedErr: "invalid LessThan version constraint invalid:",
		},
		{
			name: "complex dependency chain - should pass",
			columns: []Column{
				&mockColumn{id: "base", version: "1.0.0"},
				&mockColumn{
					id:      "intermediate",
					version: "2.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "base", GreaterOrEqualTo: "1.0.0"},
					},
				},
				&mockColumn{
					id:      "final",
					version: "3.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "intermediate", GreaterOrEqualTo: "2.0.0"},
						{Interface: "base", GreaterOrEqualTo: "1.0.0"},
					},
				},
			},
			expectedErr: "",
		},
		{
			name: "multiple dependencies with one missing - should fail",
			columns: []Column{
				&mockColumn{id: "col1", version: "1.0.0"},
				&mockColumn{
					id:      "col2",
					version: "2.0.0",
					dependsOn: []DependsOnEntry{
						{Interface: "col1"},
						{Interface: "missing"},
					},
				},
			},
			expectedErr: "column col2 depends on column missing, which is not present",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			columns := tt.columns

			// when
			err := AssertAllDependenciesFulfilled(columns)

			// then
			if tt.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}
