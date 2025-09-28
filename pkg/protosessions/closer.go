// Package protosessions provides functionality for aggregating hits into proto-sessions
package protosessions

import (
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/sirupsen/logrus"
)

// Closer defines an interface for closing and processing hit sessions
type Closer interface {
	Close(protosession []*hits.Hit) error
}

type printingCloser struct {
}

func (c *printingCloser) Close(protosession []*hits.Hit) error {
	for _, hit := range protosession {
		logrus.Warnf("Closing protosession: /%v/%v/%v", hit.AuthoritativeClientID, hit.ClientID, hit.ID)
	}
	return nil
}

// NewPrintingCloser creates a new Closer implementation that prints the hits to stdout
func NewPrintingCloser() Closer {
	return &printingCloser{}
}
