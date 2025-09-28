package columns

// BrokenSessionError is an error that occurs when a session is invalid.
type BrokenSessionError struct {
	message string
}

func (e *BrokenSessionError) Error() string {
	return e.message
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

// NewBrokenEventError creates a new BrokenEventError.
func NewBrokenEventError(message string) *BrokenEventError {
	return &BrokenEventError{message: message}
}
