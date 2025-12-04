package receiver

import "github.com/d8a-tech/d8a/pkg/hits"

type testStorage struct {
	pushFunc func(hits []*hits.Hit) error
}

func (t *testStorage) Push(hits []*hits.Hit) error {
	return t.pushFunc(hits)
}

// NewTestStorage creates a new test storage instance with the given push function.
func NewTestStorage(pushFunc func(hits []*hits.Hit) error) Storage {
	ts := &testStorage{
		pushFunc: pushFunc,
	}
	return ts
}
