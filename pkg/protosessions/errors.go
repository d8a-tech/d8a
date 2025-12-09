package protosessions

type ProtosessionError interface {
	Error() string
	IsRetryable() bool
}

type protosessionErrorImpl struct {
	err         error
	isRetryable bool
}

func (e *protosessionErrorImpl) Error() string {
	return e.err.Error()
}

func (e *protosessionErrorImpl) IsRetryable() bool {
	return e.isRetryable
}

func NewErrorCausingTaskDrop(err error) ProtosessionError {
	return &protosessionErrorImpl{
		err:         err,
		isRetryable: false,
	}
}

func NewErrorCausingTaskRetry(err error) ProtosessionError {
	return &protosessionErrorImpl{
		err:         err,
		isRetryable: true,
	}
}
