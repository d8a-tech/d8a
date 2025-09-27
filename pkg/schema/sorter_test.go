package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDependencySorter_SortAllColumns(t *testing.T) {
	tests := []struct {
		name          string
		columns       Columns
		expectedOrder []InterfaceID
		expectedError string
	}{
		{
			name: "session and event columns with cross-type dependency",
			columns: NewColumns(
				[]SessionColumn{
					&mockSessionColumn{
						id:      "session_depends_on_event",
						version: "1.0.0",
						field:   &arrow.Field{Name: "session_depends_on_event", Type: arrow.BinaryTypes.String},
						dependsOn: []DependsOnEntry{
							{Interface: "base_event"},
						},
					},
				},
				[]EventColumn{
					&mockEventColumn{
						id:      "base_event",
						version: "1.0.0",
						field:   &arrow.Field{Name: "base_event", Type: arrow.BinaryTypes.String},
					},
				},
			),
			// Cross-type dependencies should be validated, but columns are returned grouped by type
			expectedOrder: []InterfaceID{"session_depends_on_event", "base_event"},
		},
		{
			name: "event depends on session",
			columns: NewColumns(
				[]SessionColumn{
					&mockSessionColumn{
						id:      "base_session",
						version: "1.0.0",
						field:   &arrow.Field{Name: "base_session", Type: arrow.BinaryTypes.String},
					},
				},
				[]EventColumn{
					&mockEventColumn{
						id:      "event_depends_on_session",
						version: "1.0.0",
						field:   &arrow.Field{Name: "event_depends_on_session", Type: arrow.BinaryTypes.String},
						dependsOn: []DependsOnEntry{
							{Interface: "base_session"},
						},
					},
				},
			),
			expectedOrder: []InterfaceID{"base_session", "event_depends_on_session"},
		},
		{
			name: "complex cross-type dependencies",
			columns: NewColumns(
				[]SessionColumn{
					&mockSessionColumn{
						id:      "session_b",
						version: "1.0.0",
						field:   &arrow.Field{Name: "session_b", Type: arrow.BinaryTypes.String},
						dependsOn: []DependsOnEntry{
							{Interface: "event_a"},
						},
					},
					&mockSessionColumn{
						id:      "session_a",
						version: "1.0.0",
						field:   &arrow.Field{Name: "session_a", Type: arrow.BinaryTypes.String},
					},
				},
				[]EventColumn{
					&mockEventColumn{
						id:      "event_b",
						version: "1.0.0",
						field:   &arrow.Field{Name: "event_b", Type: arrow.BinaryTypes.String},
						dependsOn: []DependsOnEntry{
							{Interface: "session_a"},
						},
					},
					&mockEventColumn{
						id:      "event_a",
						version: "1.0.0",
						field:   &arrow.Field{Name: "event_a", Type: arrow.BinaryTypes.String},
					},
				},
			),
			// Columns are grouped by type, with dependencies respected within each type
			expectedOrder: []InterfaceID{"session_a", "session_b", "event_a", "event_b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			sorter := NewDependencySorter()

			// when
			result, err := sorter.SortAllColumns(tt.columns)

			// then
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				return
			}

			require.NoError(t, err)

			// Verify the total order across both session and event columns
			allResultColumns := make([]Column, 0, len(result.Session)+len(result.Event))
			for _, col := range result.Session {
				allResultColumns = append(allResultColumns, col)
			}
			for _, col := range result.Event {
				allResultColumns = append(allResultColumns, col)
			}

			actualOrder := make([]InterfaceID, len(allResultColumns))
			for i, col := range allResultColumns {
				actualOrder[i] = col.Implements().ID
			}
			assert.Equal(t, tt.expectedOrder, actualOrder)
		})
	}
}
