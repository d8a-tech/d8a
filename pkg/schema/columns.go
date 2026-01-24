package schema

import (
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
)

// InterfaceID represents a unique identifier for a column.
type InterfaceID string

// DependsOnEntry represents a dependency requirement for a column.
type DependsOnEntry struct {
	Interface InterfaceID
}

// Interface represents a virtual column, with given name and a field (including column name and type)
// Various parts of the system may then provide implementations of this interface. As example you can use something
// like "event_type". You may centrally define an abstract column interface, and then provide
// implementations in the protocols, which may have different understanding of what really means "event_type".
type Interface struct {
	ID    InterfaceID
	Field *arrow.Field
}

// Documentation represents the documentation for a column.
type Documentation struct {
	ColumnName  string
	Type        *arrow.Field
	DisplayName string
	InterfaceID string
	Description string
}

// Column represents a column with metadata and dependencies.
type Column interface {
	Docs() Documentation
	Implements() Interface
	DependsOn() []DependsOnEntry
}

// Columns groups the column sources for a table layout
type Columns struct {
	Session            []SessionColumn
	Event              []EventColumn
	SessionScopedEvent []SessionScopedEventColumn
}

// Copy returns a copy of the Columns struct.
func (c Columns) Copy() Columns {
	return Columns{
		Session:            slices.Clone(c.Session),
		Event:              slices.Clone(c.Event),
		SessionScopedEvent: slices.Clone(c.SessionScopedEvent),
	}
}

// NewColumns creates a new Columns struct with session, event and session-scoped-event columns.
func NewColumns(session []SessionColumn, event []EventColumn, sessionScoped []SessionScopedEventColumn) Columns {
	return Columns{
		Session:            session,
		Event:              event,
		SessionScopedEvent: sessionScoped,
	}
}

// FromColumns creates an Arrow schema from a slice of columns.
func FromColumns[T Column](columns []T) *arrow.Schema {
	return WithExtraFields(columns)
}

// WithExtraFields creates an Arrow schema from columns with additional fields.
func WithExtraFields[T Column](columns []T, extraFields ...arrow.Field) *arrow.Schema {
	fields := make([]arrow.Field, len(columns), len(columns)+len(extraFields))
	for i, column := range columns {
		fields[i] = *column.Implements().Field
	}
	fields = append(fields, extraFields...)
	return arrow.NewSchema(fields, nil)
}

// ToGenericColumns converts a slice of typed columns to a slice of generic Column interfaces.
func ToGenericColumns[T Column](columns []T) []Column {
	genericColumns := make([]Column, len(columns))
	for i, column := range columns {
		genericColumns[i] = column
	}
	return genericColumns
}
