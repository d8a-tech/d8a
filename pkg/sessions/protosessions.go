// Package sessions provides session management functionality for the tracking system.
package sessions

import (
	"sort"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

// SessionWriter defines the interface for writing sessions
type SessionWriter interface {
	Write(sessions ...*schema.Session) error
}

type DirectCloser struct {
	failureSleepDuration time.Duration
	writer               SessionWriter
}

// Close implements protosessions.Closer
func (c *DirectCloser) Close(protosessions [][]*hits.Hit) error {
	sessions := make([]*schema.Session, 0, len(protosessions))

	for _, protosession := range protosessions {
		if len(protosession) == 0 {
			continue
		}

		// Sort events by server received time
		sort.Slice(protosession, func(i, j int) bool {
			return protosession[i].MustParsedRequest().
				ServerReceivedTime.Before(
				protosession[j].MustParsedRequest().ServerReceivedTime,
			)
		})

		events := make([]*schema.Event, len(protosession))
		for i, hit := range protosession {
			events[i] = &schema.Event{
				BoundHit: hit,
				Metadata: make(map[string]any),
				Values:   make(map[string]any),
			}
		}
		sessions = append(sessions, schema.NewSession(events))
	}

	if len(sessions) == 0 {
		return nil
	}

	// Group sessions by PropertyID to ensure writer.Write() never receives mixed-property batches
	sessionsByProperty := make(map[string][]*schema.Session)
	for _, session := range sessions {
		propertyID := session.PropertyID
		sessionsByProperty[propertyID] = append(sessionsByProperty[propertyID], session)
	}

	// Write each property group separately (fail-fast: return on first error)
	// Use deterministic property ordering to avoid flaky behavior caused by Go map iteration order.
	propertyIDs := make([]string, 0, len(sessionsByProperty))
	for propertyID := range sessionsByProperty {
		propertyIDs = append(propertyIDs, propertyID)
	}
	sort.Strings(propertyIDs)

	for _, propertyID := range propertyIDs {
		propertySessions := sessionsByProperty[propertyID]
		if err := c.writer.Write(propertySessions...); err != nil {
			logrus.Errorf(
				"failed to write sessions for property %q: %v, adding Sleep to avoid spamming the warehouse",
				propertyID,
				err)
			time.Sleep(c.failureSleepDuration)
			return err
		}
	}

	return nil
}

// NewDirectCloser creates a new protosessions.Closer that writes the session directly to
// warehouse.Driver, without intermediate queue (suitable only for single-tenant setup)
func NewDirectCloser(writer SessionWriter, failureSleepDuration time.Duration) *DirectCloser {
	return &DirectCloser{
		failureSleepDuration: failureSleepDuration,
		writer:               writer,
	}
}
