package schema

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockEventColumn implements EventColumn for testing
type mockEventColumn struct {
	id        InterfaceID
	version   Version
	field     *arrow.Field
	dependsOn []DependsOnEntry
	writeErr  error
}

func (m *mockEventColumn) Docs() Documentation {
	return Documentation{
		ColumnName:  "Mock event column",
		Description: "Mock event column description",
	}
}

func (m *mockEventColumn) Implements() Interface {
	return Interface{
		ID:      m.id,
		Version: m.version,
		Field:   m.field,
	}
}
func (m *mockEventColumn) DependsOn() []DependsOnEntry { return m.dependsOn }

func (m *mockEventColumn) Write(event *Event) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	event.Values[m.field.Name] = "test_value"
	return nil
}

// mockSessionColumn implements SessionColumn for testing
type mockSessionColumn struct {
	id        InterfaceID
	version   Version
	field     *arrow.Field
	dependsOn []DependsOnEntry
	writeErr  error
}

func (m *mockSessionColumn) Docs() Documentation {
	return Documentation{
		ColumnName:  "Mock session column",
		Description: "Mock session column description",
	}
}

func (m *mockSessionColumn) Implements() Interface {
	return Interface{
		ID:      m.id,
		Version: m.version,
		Field:   m.field,
	}
}
func (m *mockSessionColumn) DependsOn() []DependsOnEntry { return m.dependsOn }

func (m *mockSessionColumn) Write(session *Session) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	session.Values[m.field.Name] = "session_value"
	return nil
}

func TestEventsWithEmbeddedSessionColumnsTableLayout_Tables(t *testing.T) {
	tests := []struct {
		name             string
		eventsTableName  string
		sessionPrefix    string
		eventColumns     []EventColumn
		sessionColumns   []SessionColumn
		expectedTableNum int
		expectedName     string
		expectedFields   []string
	}{
		{
			name:            "single event and session column",
			eventsTableName: "events",
			sessionPrefix:   "session_",
			eventColumns: []EventColumn{
				&mockEventColumn{
					id:      "event_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "event_id", Type: arrow.BinaryTypes.String},
				},
			},
			sessionColumns: []SessionColumn{
				&mockSessionColumn{
					id:      "session_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
				},
			},
			expectedTableNum: 1,
			expectedName:     "events",
			expectedFields:   []string{"event_id", "session_session_id"},
		},
		{
			name:            "multiple columns with custom prefix",
			eventsTableName: "custom_events",
			sessionPrefix:   "sess_",
			eventColumns: []EventColumn{
				&mockEventColumn{
					id:      "event_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "event_id", Type: arrow.BinaryTypes.String},
				},
				&mockEventColumn{
					id:      "event_type",
					version: "1.0.0",
					field:   &arrow.Field{Name: "event_type", Type: arrow.BinaryTypes.String},
				},
			},
			sessionColumns: []SessionColumn{
				&mockSessionColumn{
					id:      "session_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
				},
				&mockSessionColumn{
					id:      "user_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
				},
			},
			expectedTableNum: 1,
			expectedName:     "custom_events",
			expectedFields:   []string{"event_id", "event_type", "sess_session_id", "sess_user_id"},
		},
		{
			name:             "no columns",
			eventsTableName:  "empty_events",
			sessionPrefix:    "s_",
			eventColumns:     []EventColumn{},
			sessionColumns:   []SessionColumn{},
			expectedTableNum: 1,
			expectedName:     "empty_events",
			expectedFields:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			layout := &eventsWithEmbeddedSessionColumnsLayout{
				eventsTableName:      tt.eventsTableName,
				sessionColumnsPrefix: tt.sessionPrefix,
			}
			sources := Columns{
				Event:   tt.eventColumns,
				Session: tt.sessionColumns,
			}

			// when
			result := layout.Tables(sources)

			// then
			assert.Len(t, result, tt.expectedTableNum)
			if len(result) > 0 {
				assert.Equal(t, tt.expectedName, result[0].Table)
				assert.NotNil(t, result[0].Schema)

				actualFields := make([]string, len(result[0].Schema.Fields()))
				for i, field := range result[0].Schema.Fields() {
					actualFields[i] = field.Name
				}
				assert.ElementsMatch(t, tt.expectedFields, actualFields)
			}
		})
	}
}

func TestEventsWithEmbeddedSessionColumnsTableLayout_Batchify(t *testing.T) {
	tests := []struct {
		name              string
		eventsTableName   string
		sessionPrefix     string
		eventColumns      []EventColumn
		sessionColumns    []SessionColumn
		sessions          []*Session
		expectedBatches   int
		expectedTableName string
		expectedRowCount  int
		expectError       bool
	}{
		{
			name:            "single session with events",
			eventsTableName: "events",
			sessionPrefix:   "session_",
			eventColumns: []EventColumn{
				&mockEventColumn{
					id:      "event_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "event_id", Type: arrow.BinaryTypes.String},
				},
			},
			sessionColumns: []SessionColumn{
				&mockSessionColumn{
					id:      "session_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
				},
			},
			sessions: []*Session{
				{
					PropertyID: "prop1",
					Events: []*Event{
						{BoundHit: &hits.Hit{ID: "hit1"}, Values: map[string]any{"event_id": "test_value"}},
						{BoundHit: &hits.Hit{ID: "hit2"}, Values: map[string]any{"event_id": "test_value"}},
					},
					Values: map[string]any{"session_id": "session_value"},
				},
			},
			expectedBatches:   1,
			expectedTableName: "events",
			expectedRowCount:  2,
			expectError:       false,
		},
		{
			name:            "multiple sessions",
			eventsTableName: "events",
			sessionPrefix:   "sess_",
			eventColumns: []EventColumn{
				&mockEventColumn{
					id:      "event_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "event_id", Type: arrow.BinaryTypes.String},
				},
			},
			sessionColumns: []SessionColumn{
				&mockSessionColumn{
					id:      "session_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
				},
			},
			sessions: []*Session{
				{
					PropertyID: "prop1",
					Events: []*Event{
						{BoundHit: &hits.Hit{ID: "hit1"}, Values: map[string]any{"event_id": "test_value"}},
					},
					Values: map[string]any{"session_id": "session_value"},
				},
				{
					PropertyID: "prop2",
					Events: []*Event{
						{BoundHit: &hits.Hit{ID: "hit2"}, Values: map[string]any{"event_id": "test_value"}},
						{BoundHit: &hits.Hit{ID: "hit3"}, Values: map[string]any{"event_id": "test_value"}},
					},
					Values: map[string]any{"session_id": "session_value"},
				},
			},
			expectedBatches:   1,
			expectedTableName: "events",
			expectedRowCount:  3,
			expectError:       false,
		},
		{
			name:            "empty sessions",
			eventsTableName: "events",
			sessionPrefix:   "session_",
			eventColumns: []EventColumn{
				&mockEventColumn{
					id:      "event_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "event_id", Type: arrow.BinaryTypes.String},
				},
			},
			sessionColumns: []SessionColumn{
				&mockSessionColumn{
					id:      "session_id",
					version: "1.0.0",
					field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
				},
			},
			sessions:          []*Session{},
			expectedBatches:   1,
			expectedTableName: "events",
			expectedRowCount:  0,
			expectError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			layout := &eventsWithEmbeddedSessionColumnsLayout{
				eventsTableName:      tt.eventsTableName,
				sessionColumnsPrefix: tt.sessionPrefix,
			}
			sources := Columns{
				Event:   tt.eventColumns,
				Session: tt.sessionColumns,
			}

			// when
			result, err := layout.ToRows(sources, tt.sessions...)

			// then
			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, result, tt.expectedBatches)
			if len(result) > 0 {
				assert.Equal(t, tt.expectedTableName, result[0].Table)
				assert.Len(t, result[0].Rows, tt.expectedRowCount)
			}
		})
	}
}

func TestEventsWithEmbeddedSessionColumnsTableLayout_SessionValuesPropagation(t *testing.T) {
	// given
	layout := &eventsWithEmbeddedSessionColumnsLayout{
		eventsTableName:      "events",
		sessionColumnsPrefix: "session_",
	}

	eventCol := &mockEventColumn{
		id:      "event_id",
		version: "1.0.0",
		field:   &arrow.Field{Name: "event_id", Type: arrow.BinaryTypes.String},
	}
	sessionCol := &mockSessionColumn{
		id:      "session_id",
		version: "1.0.0",
		field:   &arrow.Field{Name: "session_id", Type: arrow.BinaryTypes.String},
	}

	sources := Columns{
		Event:   []EventColumn{eventCol},
		Session: []SessionColumn{sessionCol},
	}

	session := &Session{
		PropertyID: "prop1",
		Events: []*Event{
			{BoundHit: &hits.Hit{ID: "hit1"}, Values: map[string]any{"event_id": "test_value"}},
			{BoundHit: &hits.Hit{ID: "hit2"}, Values: map[string]any{"event_id": "test_value"}},
		},
		Values: map[string]any{
			"user_id":    "test_user",     // This should be copied to events
			"session_id": "session_value", // This should be copied with prefix
		},
	}

	// when
	result, err := layout.ToRows(sources, session)

	// then
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0].Rows, 2)

	// Verify session values are embedded in event values with prefix
	for _, row := range result[0].Rows {
		// The original user_id should be copied with prefix
		assert.Equal(t, "test_user", row["session_user_id"])
		// The session_id should be copied with prefix
		assert.Equal(t, "session_value", row["session_session_id"])
		// Event values should remain unchanged
		assert.Equal(t, "test_value", row["event_id"])
	}
}

type exampleEventColumn struct {
}

func (c *exampleEventColumn) Docs() Documentation {
	return Documentation{
		ColumnName:  "Example event column",
		Description: "Example event column description",
	}
}

func (c *exampleEventColumn) Implements() Interface {
	return Interface{
		ID:      "example_event_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "example_event_id", Type: arrow.BinaryTypes.String},
	}
}

func (c *exampleEventColumn) Write(event *Event) error {
	event.Values["example_event_id"] = "test_value"
	return nil
}

func (c *exampleEventColumn) DependsOn() []DependsOnEntry {
	return []DependsOnEntry{}
}

type exampleSessionColumn struct {
}

func (c *exampleSessionColumn) Docs() Documentation {
	return Documentation{
		ColumnName:  "Example session column",
		Description: "Example session column description",
	}
}

func (c *exampleSessionColumn) Implements() Interface {
	return Interface{
		ID:      "example_session_id",
		Version: "1.0.0",
		Field:   &arrow.Field{Name: "example_session_id", Type: arrow.BinaryTypes.String},
	}
}

func (c *exampleSessionColumn) Write(session *Session) error {
	session.Values["example_session_id"] = "test_value"
	return nil
}

func (c *exampleSessionColumn) DependsOn() []DependsOnEntry {
	return []DependsOnEntry{}
}

func TestEventsWithEmbeddedSessionColumnsTableLayout_IntegrationTest(t *testing.T) {
	// given
	layout := &eventsWithEmbeddedSessionColumnsLayout{
		eventsTableName:      "analytics_events",
		sessionColumnsPrefix: "sess_",
	}

	eventColumns := []EventColumn{
		&exampleEventColumn{},
	}
	sessionColumns := []SessionColumn{
		&exampleSessionColumn{},
	}

	sources := Columns{
		Event:   eventColumns,
		Session: sessionColumns,
	}

	// Create test sessions with pre-populated data (simulating writer output)
	hit1 := hits.New()
	hit1.PropertyID = "test_property"
	hit2 := hits.New()
	hit2.PropertyID = "test_property"

	session := &Session{
		PropertyID: "test_property",
		Events: []*Event{
			{BoundHit: hit1, Values: map[string]any{"example_event_id": "event1_value"}},
			{BoundHit: hit2, Values: map[string]any{"example_event_id": "event2_value"}},
		},
		Values: map[string]any{
			"example_session_id": "session_value",
			"user_id":            "test_user",
		},
	}

	// when - test Tables method
	tables := layout.Tables(sources)

	// then
	require.Len(t, tables, 1)
	assert.Equal(t, "analytics_events", tables[0].Table)
	assert.NotNil(t, tables[0].Schema)

	actualFieldNames := make([]string, len(tables[0].Schema.Fields()))
	for i, field := range tables[0].Schema.Fields() {
		actualFieldNames[i] = field.Name
	}
	expectedFieldNames := []string{"example_event_id", "sess_example_session_id"}
	assert.ElementsMatch(t, expectedFieldNames, actualFieldNames)

	// when - test ToRows method
	batches, err := layout.ToRows(sources, session)

	// then
	require.NoError(t, err)
	require.Len(t, batches, 1)
	assert.Equal(t, "analytics_events", batches[0].Table)
	assert.Len(t, batches[0].Rows, 2)

	// Verify each row has the expected structure and data
	for i, row := range batches[0].Rows {
		// The original user_id should be copied with prefix
		assert.Equal(t, "test_user", row["sess_user_id"])
		// The session_id should be copied with prefix
		assert.Equal(t, "session_value", row["sess_example_session_id"])
		// Event values should match the event - first event has event1_value, second has event2_value
		expectedEventValue := fmt.Sprintf("event%d_value", i+1)
		assert.Equal(t, expectedEventValue, row["example_event_id"])
	}
}

// TestEventsWithEmbeddedSessionColumnsTableLayout_IdempotentCalls tests that repeated calls
// to Tables and ToRows methods don't cause duplicate prefixing (the retry bug)
func TestEventsWithEmbeddedSessionColumnsTableLayout_IdempotentCalls(t *testing.T) {
	// given
	layout := &eventsWithEmbeddedSessionColumnsLayout{
		eventsTableName:      "events",
		sessionColumnsPrefix: "session_",
	}

	eventColumns := []EventColumn{
		&mockEventColumn{
			id:      "event_id",
			version: "1.0.0",
			field:   &arrow.Field{Name: "event_id", Type: arrow.BinaryTypes.String},
		},
	}
	sessionColumns := []SessionColumn{
		&mockSessionColumn{
			id:      "session_duration",
			version: "1.0.0",
			field:   &arrow.Field{Name: "duration", Type: arrow.PrimitiveTypes.Int64},
		},
	}

	sources := Columns{
		Event:   eventColumns,
		Session: sessionColumns,
	}

	session := &Session{
		PropertyID: "test_property",
		Events: []*Event{
			{BoundHit: &hits.Hit{ID: "hit1"}, Values: map[string]any{"event_id": "test_event"}},
		},
		Values: map[string]any{
			"duration": int64(12345),
		},
	}

	// when - call Tables method multiple times (simulating retries)
	tables1 := layout.Tables(sources)
	tables2 := layout.Tables(sources)
	tables3 := layout.Tables(sources)

	// then - field names should be consistent across calls
	require.Len(t, tables1, 1)
	require.Len(t, tables2, 1)
	require.Len(t, tables3, 1)

	fieldNames1 := make([]string, len(tables1[0].Schema.Fields()))
	fieldNames2 := make([]string, len(tables2[0].Schema.Fields()))
	fieldNames3 := make([]string, len(tables3[0].Schema.Fields()))

	for i, field := range tables1[0].Schema.Fields() {
		fieldNames1[i] = field.Name
	}
	for i, field := range tables2[0].Schema.Fields() {
		fieldNames2[i] = field.Name
	}
	for i, field := range tables3[0].Schema.Fields() {
		fieldNames3[i] = field.Name
	}

	expectedFieldNames := []string{"event_id", "session_duration"}
	assert.ElementsMatch(t, expectedFieldNames, fieldNames1)
	assert.ElementsMatch(t, expectedFieldNames, fieldNames2)
	assert.ElementsMatch(t, expectedFieldNames, fieldNames3)

	// Check that we have exactly one field with the prefix (not duplicated)
	sessionFieldCount := 0
	for _, name := range fieldNames1 {
		if name == "session_duration" {
			sessionFieldCount++
		}
	}
	assert.Equal(t, 1, sessionFieldCount, "Should have exactly one session_duration field")

	// when - call ToRows method multiple times (simulating retries)
	batches1, err1 := layout.ToRows(sources, session)
	batches2, err2 := layout.ToRows(sources, session)
	batches3, err3 := layout.ToRows(sources, session)

	// then - all calls should succeed
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NoError(t, err3)

	// then - row data should be consistent across calls
	require.Len(t, batches1, 1)
	require.Len(t, batches2, 1)
	require.Len(t, batches3, 1)

	require.Len(t, batches1[0].Rows, 1)
	require.Len(t, batches2[0].Rows, 1)
	require.Len(t, batches3[0].Rows, 1)

	row1 := batches1[0].Rows[0]
	row2 := batches2[0].Rows[0]
	row3 := batches3[0].Rows[0]

	// Check that all rows have the same structure
	assert.Equal(t, row1, row2)
	assert.Equal(t, row2, row3)

	// Check that we have exactly one field with the prefix (not duplicated)
	expectedSessionValue := int64(12345)
	assert.Equal(t, expectedSessionValue, row1["session_duration"])
	assert.Equal(t, expectedSessionValue, row2["session_duration"])
	assert.Equal(t, expectedSessionValue, row3["session_duration"])

	// Verify there's no duplicate prefixing like "session_session_duration"
	assert.NotContains(t, row1, "session_session_duration")
	assert.NotContains(t, row2, "session_session_duration")
	assert.NotContains(t, row3, "session_session_duration")

	// Verify original field values are preserved
	assert.Equal(t, "test_event", row1["event_id"])
	assert.Equal(t, "test_event", row2["event_id"])
	assert.Equal(t, "test_event", row3["event_id"])
}
