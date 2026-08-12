package receiver

// safeClientError is safe to return to tracker clients.
type safeClientError interface {
	error
	clientMessage() string
}

type clientError struct {
	message string
}

func (e *clientError) Error() string {
	return e.message
}

func (e *clientError) clientMessage() string {
	return e.message
}

func newClientError(message string) safeClientError {
	return &clientError{message: message}
}
