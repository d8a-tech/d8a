package worker

import "github.com/sirupsen/logrus"

// ErrorType is the type of transformation error
type ErrorType string

const (
	// ErrTypeDroppable is the type of transformation error when a hit should be dropped
	ErrTypeDroppable ErrorType = "drop"
	// ErrTypeRetryable is the type of transformation error when a hit can be retried
	ErrTypeRetryable ErrorType = "retry"
)

// Error is an error that occurs during task processing and must must be handled in one of the
// predefined ways. We do not allow returnning generic error interface, as it leaves too much
// flexibility to the caller.
// Middleware authors may introduce their own types, that must however be handled in the middleware itself.
type Error struct {
	Type ErrorType
	Err  error
}

func (e *Error) Error() string {
	return e.Err.Error()
}

// NewError wraps a standard error with a type that can be used to determine how to handle it
func NewError(typ ErrorType, err error) *Error {
	if err == nil {
		logrus.Panicf("error is nil")
	}
	return &Error{
		Type: typ,
		Err:  err,
	}
}
