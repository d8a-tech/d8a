// Package sessions provides session management functionality for the tracking system.
package sessions

import (
	"sort"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protosessions"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

// SessionWriter defines the interface for writing sessions
type SessionWriter interface {
	Write(sessions ...*schema.Session) error
}

type directCloser struct {
	failureSleepDuration time.Duration
	writer               SessionWriter
}

func (c *directCloser) Close(protosession []*hits.Hit) error {
	// Handle empty protosession
	if len(protosession) == 0 {
		return nil
	}

	// Sort events by server received time
	sort.Slice(protosession, func(i, j int) bool {
		return protosession[i].ServerReceivedTime.Before(protosession[j].ServerReceivedTime)
	})

	session := &schema.Session{
		Metadata:   make(map[string]any),
		Events:     make([]*schema.Event, len(protosession)),
		PropertyID: protosession[0].PropertyID,
		Values:     make(map[string]any),
	}
	for i, hit := range protosession {
		session.Events[i] = &schema.Event{
			BoundHit: hit,
			Metadata: make(map[string]any),
			Values:   make(map[string]any),
		}
	}

	if err := c.writer.Write(session); err != nil {
		logrus.Errorf("failed to write session: %v, adding Sleep to avoid spamming the warehouse", err)
		time.Sleep(c.failureSleepDuration)
		return err
	}

	return nil
}

// NewDirectCloser creates a new protosessions.Closer that writes the session directly to
// warehouse.Driver, without intermediate queue (suitable only for single-tenant setup)
func NewDirectCloser(writer SessionWriter, failureSleepDuration time.Duration) protosessions.Closer {
	return &directCloser{
		failureSleepDuration: failureSleepDuration,
		writer:               writer,
	}
}
