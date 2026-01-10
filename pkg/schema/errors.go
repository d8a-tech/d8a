package schema

type D8AColumnWriteError interface {
	Error() string
	IsRetryable() bool
}

// BrokenSessionError is an error that occurs when a session is invalid.
type BrokenSessionError struct {
	message string
}

func (e *BrokenSessionError) Error() string {
	return e.message
}

func (e *BrokenSessionError) IsRetryable() bool {
	return false
}

// NewBrokenSessionError creates a new InvalidSessionError.
func NewBrokenSessionError(message string) *BrokenSessionError {
	return &BrokenSessionError{message: message}
}

// BrokenEventError is an error that occurs when an event doesn't have
//
//	all required information in order to be processed.
type BrokenEventError struct {
	message string
}

func (e *BrokenEventError) Error() string {
	return e.message
}

func (e *BrokenEventError) IsRetryable() bool {
	return false
}

// NewBrokenEventError creates a new BrokenEventError.
func NewBrokenEventError(message string) *BrokenEventError {
	return &BrokenEventError{message: message}
}

type RetryableError struct {
	message string
}

func (e *RetryableError) Error() string {
	return e.message
}

func (e *RetryableError) IsRetryable() bool {
	return true
}

func NewRetryableError(message string) *RetryableError {
	return &RetryableError{message: message}
}
