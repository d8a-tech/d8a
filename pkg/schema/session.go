package schema

// SessionColumn represents a column that can be written to during session processing.
type SessionColumn interface {
	Column
	Write(session *Session) D8AColumnWriteError
}
