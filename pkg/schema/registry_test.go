package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDependencySorter_sortColumns(t *testing.T) {
	tests := []struct {
		name          string
		columns       []Column
		expectedOrder []InterfaceID
		expectedError string
	}{
		{
			name:          "empty columns",
			columns:       []Column{},
			expectedOrder: []InterfaceID{},
		},
		{
			name: "single column",
			columns: []Column{
				&mockEventColumn{
					id:      "id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "id", Type: arrow.BinaryTypes.String},
				},
			},
			expectedOrder: []InterfaceID{"id"},
		},
		{
			name: "multiple independent columns maintain order",
			columns: []Column{
				&mockEventColumn{
					id:      "id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "id", Type: arrow.BinaryTypes.String},
				},
				&mockSessionColumn{
					id:      "session_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
				},
			},
			expectedOrder: []InterfaceID{"id", "session_id"},
		},
		{
			name: "mixed column types with dependencies",
			columns: []Column{
				&mockEventColumn{
					id:      "derived_event",
					version: "1.0.0",
					field:   &arrow.Field{Name: "derived_event", Type: arrow.BinaryTypes.String},
					dependsOn: []DependsOnEntry{
						{Interface: "base_session"},
					},
				},
				&mockSessionColumn{
					id:      "base_session",
					version: "1.0.0",
					field:   &arrow.Field{Name: "base_session", Type: arrow.BinaryTypes.String},
				},
			},
			expectedOrder: []InterfaceID{"base_session", "derived_event"},
		},
		{
			name: "circular dependency detection",
			columns: []Column{
				&mockEventColumn{
					id:      "col_a",
					version: "1.0.0",
					field:   &arrow.Field{Name: "col_a", Type: arrow.BinaryTypes.String},
					dependsOn: []DependsOnEntry{
						{Interface: "col_b"},
					},
				},
				&mockEventColumn{
					id:      "col_b",
					version: "1.0.0",
					field:   &arrow.Field{Name: "col_b", Type: arrow.BinaryTypes.String},
					dependsOn: []DependsOnEntry{
						{Interface: "col_a"},
					},
				},
			},
			expectedError: "circular dependency detected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			sorter := NewDependencySorter()

			// when
			result, err := sorter.sortColumns(tt.columns)

			// then
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				return
			}

			require.NoError(t, err)
			assert.Len(t, result, len(tt.expectedOrder))

			actualOrder := make([]InterfaceID, len(result))
			for i, col := range result {
				actualOrder[i] = col.Implements().ID
			}
			assert.Equal(t, tt.expectedOrder, actualOrder)
		})
	}
}

func TestStaticColumnsRegistry_Get(t *testing.T) {
	tests := []struct {
		name            string
		propertyID      string
		registryColumns map[string]Columns
		defaultColumns  Columns
		expectedColumns Columns
		expectedError   string
	}{
		{
			name:       "existing property returns specific columns",
			propertyID: "prop1",
			registryColumns: map[string]Columns{
				"prop1": NewColumns(
					[]SessionColumn{
						&mockSessionColumn{
							id:      "session_id",
							version: "1.0.0",
							field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
						},
					},
					[]EventColumn{
						&mockEventColumn{
							id:      "id",
							version: "1.0.0",
							field:   &arrow.Field{Name: "id", Type: arrow.BinaryTypes.String},
						},
					},
					[]SessionScopedEventColumn{},
				),
			},
			defaultColumns: NewColumns(
				[]SessionColumn{
					&mockSessionColumn{
						id:      "default_session",
						version: "1.0.0",
						field:   &arrow.Field{Name: "default_session", Type: arrow.BinaryTypes.String},
					},
				},
				[]EventColumn{},
				[]SessionScopedEventColumn{},
			),
			expectedColumns: NewColumns(
				[]SessionColumn{
					&mockSessionColumn{
						id:      "session_id",
						version: "1.0.0",
						field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
					},
				},
				[]EventColumn{
					&mockEventColumn{
						id:      "id",
						version: "1.0.0",
						field:   &arrow.Field{Name: "id", Type: arrow.BinaryTypes.String},
					},
				},
				[]SessionScopedEventColumn{},
			),
		},
		{
			name:            "non-existing property returns default columns",
			propertyID:      "non_existing",
			registryColumns: map[string]Columns{},
			defaultColumns: NewColumns(
				[]SessionColumn{
					&mockSessionColumn{
						id:      "default_session",
						version: "1.0.0",
						field:   &arrow.Field{Name: "default_session", Type: arrow.BinaryTypes.String},
					},
				},
				[]EventColumn{},
				[]SessionScopedEventColumn{},
			),
			expectedColumns: NewColumns(
				[]SessionColumn{
					&mockSessionColumn{
						id:      "default_session",
						version: "1.0.0",
						field:   &arrow.Field{Name: "default_session", Type: arrow.BinaryTypes.String},
					},
				},
				[]EventColumn{},
				[]SessionScopedEventColumn{},
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			registry := NewStaticColumnsRegistry(tt.registryColumns, tt.defaultColumns)

			// when
			result, err := registry.Get(tt.propertyID)

			// then
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				return
			}

			require.NoError(t, err)
			assert.Len(t, result.Session, len(tt.expectedColumns.Session))
			assert.Len(t, result.Event, len(tt.expectedColumns.Event))

			// Compare IDs to avoid deep struct comparison issues
			for i, col := range result.Session {
				assert.Equal(t, tt.expectedColumns.Session[i].Implements().ID, col.Implements().ID)
			}
			for i, col := range result.Event {
				assert.Equal(t, tt.expectedColumns.Event[i].Implements().ID, col.Implements().ID)
			}
		})
	}
}
