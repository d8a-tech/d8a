package receiver

import (
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/sirupsen/logrus"
)

func (s *Server) storeRawLogs(original *hits.ParsedRequest, batch []*hits.Hit, normalized bool) {
	if normalized {
		for _, hit := range batch {
			if err := s.rawLogStorage.Store(hit.Request); err != nil {
				logrus.Warn("failed to store normalized raw log")
			}
		}
		return
	}
	request := original
	if len(batch) > 0 {
		request = batch[0].Request
	}
	if err := s.rawLogStorage.Store(request); err != nil {
		logrus.Errorf("failed to store raw log: %v", err)
	}
}

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
