package schema

// SessionScopedEventColumn represents a column computed with session scope
// that writes per-event values while having access to the whole session.
// It complements EventColumn and SessionColumn without changing their semantics.
type SessionScopedEventColumn interface {
	Column
	Write(session *Session, i int) error
}
