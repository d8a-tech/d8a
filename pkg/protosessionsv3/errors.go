package protosessionsv3

type ProtosessionError struct {
	err     error
	isFatal bool
}

func (e *ProtosessionError) Error() string {
	return e.err.Error()
}

func (e *ProtosessionError) IsFatal() bool {
	return e.isFatal
}

func NewProtosessionError(err error, isFatal bool) *ProtosessionError {
	return &ProtosessionError{
		err:     err,
		isFatal: isFatal,
	}
}
