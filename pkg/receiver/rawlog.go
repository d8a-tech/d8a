package receiver

import "github.com/d8a-tech/d8a/pkg/hits"

// RawLogStorage defines the interface for storing raw log data
type RawLogStorage interface {
	Store(*hits.ParsedRequest) error
}

var _ RawLogStorage = &NoopRawLogStorage{}

// NoopRawLogStorage discards all raw log data.
type NoopRawLogStorage struct{}

// Store implements RawLogStorage
func (n *NoopRawLogStorage) Store(_ *hits.ParsedRequest) error {
	return nil
}

// NewNoopRawLogStorage creates a noop raw log storage that discards all data.
func NewNoopRawLogStorage() RawLogStorage {
	return &NoopRawLogStorage{}
}

type dummyRawLogStorage struct{}

func (d *dummyRawLogStorage) Store(_ *hits.ParsedRequest) error {
	return nil
}

// NewDummyRawLogStorage creates a dummy raw log storage that discards all data.
func NewDummyRawLogStorage() RawLogStorage {
	return &dummyRawLogStorage{}
}
