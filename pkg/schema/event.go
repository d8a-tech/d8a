package schema

// EventColumn represents a column that can be written to during event processing.
type EventColumn interface {
	Column
	Write(event *Event) error
}
