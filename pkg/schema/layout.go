package schema

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// Layout is the interface for a table layout, implementations take control over
// the final schema and dictate the format of writing the session data to the table.
type Layout interface {
	Tables(columns Columns) []WithName
	ToRows(columns Columns, sessions ...*Session) ([]TableRows, error)
}

// WithName adds a table name to the schema
type WithName struct {
	Schema *arrow.Schema
	Table  string
}

// TableRows are a collection of rows with a table to write them to
type TableRows struct {
	Table string
	Rows  []map[string]any
}

// LayoutRegistry is a registry of layouts for different properties.
type LayoutRegistry interface {
	Get(propertyID string) (Layout, error)
}

type eventsWithEmbeddedSessionColumnsLayout struct {
	eventsTableName      string
	sessionColumnsPrefix string
}

// NewEmbeddedSessionColumnsLayout creates a single table, with
// all the session columns embedded in the event table, with given prefix.
func NewEmbeddedSessionColumnsLayout(
	eventsTableName string,
	sessionColumnsPrefix string,
) Layout {
	return &eventsWithEmbeddedSessionColumnsLayout{
		eventsTableName:      eventsTableName,
		sessionColumnsPrefix: sessionColumnsPrefix,
	}
}

func (m *eventsWithEmbeddedSessionColumnsLayout) Tables(
	columns Columns,
) []WithName {
	eventColumns := columns.Event
	sessionColumns := columns.Session
	// First, include fields from session-scoped event columns as event-level fields
	ssecExtraFields := make([]arrow.Field, len(columns.SessionScopedEvent))
	for i, ssec := range columns.SessionScopedEvent {
		f := ssec.Implements().Field
		ssecExtraFields[i] = arrow.Field{
			Name:     f.Name,
			Type:     f.Type,
			Nullable: f.Nullable,
			Metadata: f.Metadata,
		}
	}

	// Then, include prefixed session fields
	prefixedSessionExtraFields := make([]arrow.Field, len(sessionColumns))
	for i, sessionColumn := range sessionColumns {
		f := sessionColumn.Implements().Field
		fieldCopy := arrow.Field{
			Name:     m.sessionColumnsPrefix + f.Name,
			Type:     f.Type,
			Nullable: f.Nullable,
			Metadata: f.Metadata,
		}
		prefixedSessionExtraFields[i] = fieldCopy
	}

	// Merge SSEC fields first (as event-level), then prefixed session fields
	allExtra := make([]arrow.Field, 0, len(ssecExtraFields)+len(prefixedSessionExtraFields))
	allExtra = append(allExtra, ssecExtraFields...)
	allExtra = append(allExtra, prefixedSessionExtraFields...)

	schema := WithExtraFields(eventColumns, allExtra...)

	return []WithName{
		{Schema: schema, Table: m.eventsTableName},
	}
}

func (m *eventsWithEmbeddedSessionColumnsLayout) ToRows(
	_ Columns,
	sessions ...*Session,
) ([]TableRows, error) {
	allValues := make([]map[string]any, 0, len(sessions))
	for _, session := range sessions {
		for _, event := range session.Events {
			eventValuesCopy := make(map[string]any)
			for k, v := range event.Values {
				eventValuesCopy[k] = v
			}
			for k, v := range session.Values {
				eventValuesCopy[fmt.Sprintf("%s%s", m.sessionColumnsPrefix, k)] = v
			}
			allValues = append(allValues, eventValuesCopy)
		}
	}
	return []TableRows{
		{Table: m.eventsTableName, Rows: allValues},
	}, nil
}

type staticLayoutRegistry struct {
	layouts       map[string]Layout
	defaultLayout Layout
}

func (m *staticLayoutRegistry) Get(propertyID string) (Layout, error) {
	layout, ok := m.layouts[propertyID]
	if !ok {
		return m.defaultLayout, nil
	}
	return layout, nil
}

// NewStaticLayoutRegistry creates a new static layout registry with the given layouts.
func NewStaticLayoutRegistry(layouts map[string]Layout, defaultLayout Layout) LayoutRegistry {
	return &staticLayoutRegistry{layouts: layouts, defaultLayout: defaultLayout}
}
