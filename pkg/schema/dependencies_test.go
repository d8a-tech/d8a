package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

// mockColumn implements the Column interface for testing
type mockColumn struct {
	id        InterfaceID
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
		ID:    m.id,
		Field: m.field,
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
				&mockColumn{id: "core.d8a.tech/events/id"},
				&mockColumn{id: "core.d8a.tech/events/type"},
				&mockColumn{id: "other.column"},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id"},
				{ID: "core.d8a.tech/events/type"},
			},
			expectedErr: "",
		},
		{
			name: "missing core column - should fail",
			columns: []Column{
				&mockColumn{id: "core.d8a.tech/events/id"},
				&mockColumn{id: "other.column"},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id"},
				{ID: "core.d8a.tech/events/type"},
			},
			expectedErr: "core column core.d8a.tech/events/type is required but not present in columns",
		},
		{
			name: "no core columns required - should pass",
			columns: []Column{
				&mockColumn{id: "some.column"},
			},
			coreColumns: []Interface{},
			expectedErr: "",
		},
		{
			name:    "empty columns with core columns required - should fail",
			columns: []Column{},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id"},
			},
			expectedErr: "core column core.d8a.tech/events/id is required but not present in columns",
		},
		{
			name: "multiple missing core columns - should fail on first",
			columns: []Column{
				&mockColumn{id: "other.column"},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id"},
				{ID: "core.d8a.tech/events/type"},
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
				&mockColumn{id: "core.d8a.tech/events/id"},
				&mockColumn{id: "core.d8a.tech/events/type"},
				&mockColumn{
					id: "dependent.column",
					dependsOn: []DependsOnEntry{
						{Interface: "core.d8a.tech/events/id"},
					},
				},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id"},
				{ID: "core.d8a.tech/events/type"},
			},
			expectedErr: "",
		},
		{
			name: "missing core column - should fail before dependency check",
			columns: []Column{
				&mockColumn{id: "core.d8a.tech/events/id"},
				&mockColumn{
					id: "dependent.column",
					dependsOn: []DependsOnEntry{
						{Interface: "core.d8a.tech/events/id"},
					},
				},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id"},
				{ID: "core.d8a.tech/events/type"},
			},
			expectedErr: "core column core.d8a.tech/events/type is required but not present in columns",
		},
		{
			name: "core columns present but dependency missing - should fail on dependency",
			columns: []Column{
				&mockColumn{id: "core.d8a.tech/events/id"},
				&mockColumn{id: "core.d8a.tech/events/type"},
				&mockColumn{
					id: "dependent.column",
					dependsOn: []DependsOnEntry{
						{Interface: "missing.dependency"},
					},
				},
			},
			coreColumns: []Interface{
				{ID: "core.d8a.tech/events/id"},
				{ID: "core.d8a.tech/events/type"},
			},
			expectedErr: "column dependent.column depends on column missing.dependency, which is not present",
		},
		{
			name: "no core columns required - should validate dependencies only",
			columns: []Column{
				&mockColumn{id: "col1"},
				&mockColumn{
					id: "col2",
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
				&mockColumn{id: "col1"},
				&mockColumn{id: "col2"},
			},
			expectedErr: "",
		},
		{
			name: "simple dependency satisfied - should pass",
			columns: []Column{
				&mockColumn{id: "col1"},
				&mockColumn{
					id: "col2",
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
					id: "col1",
					dependsOn: []DependsOnEntry{
						{Interface: "missing_col"},
					},
				},
			},
			expectedErr: "column col1 depends on column missing_col, which is not present",
		},
		{
			name: "complex dependency chain - should pass",
			columns: []Column{
				&mockColumn{id: "base"},
				&mockColumn{
					id: "intermediate",
					dependsOn: []DependsOnEntry{
						{Interface: "base"},
					},
				},
				&mockColumn{
					id: "final",
					dependsOn: []DependsOnEntry{
						{Interface: "intermediate"},
						{Interface: "base"},
					},
				},
			},
			expectedErr: "",
		},
		{
			name: "multiple dependencies with one missing - should fail",
			columns: []Column{
				&mockColumn{id: "col1"},
				&mockColumn{
					id: "col2",
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
