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

// AnnotationEnricher derives enrichment for a proto-session's events from its annotation hits
// (see hits.RoleAnnotation), before any column runs. It may only add namespaced, gob-safe values
// (strings) to event.Metadata, and must not mutate the hits: a retried close must give the same
// result. Annotations without a fitting event contribute nothing.
type AnnotationEnricher func(events []*schema.Event, annotations []*hits.Hit)

type DirectCloser struct {
	failureSleepDuration time.Duration
	writer               SessionWriter
	enrichAnnotations    AnnotationEnricher
}

// DirectCloserOption configures a DirectCloser.
type DirectCloserOption func(*DirectCloser)

// WithAnnotationEnricher lets annotation hits enrich the events of their session. Without it,
// annotations are ignored. Either way they never become events.
func WithAnnotationEnricher(enricher AnnotationEnricher) DirectCloserOption {
	return func(c *DirectCloser) {
		c.enrichAnnotations = enricher
	}
}

// Close implements protosessions.Closer
func (c *DirectCloser) Close(protosessions [][]*hits.Hit) error {
	sessions := make([]*schema.Session, 0, len(protosessions))

	for _, protosession := range protosessions {
		// Only ordinary hits become events; annotations at most enrich them.
		ordinary, annotations := splitAnnotations(protosession)
		if len(ordinary) == 0 {
			continue
		}

		// Sort events by server received time
		sort.SliceStable(ordinary, func(i, j int) bool {
			return ordinary[i].MustParsedRequest().
				ServerReceivedTime.Before(
				ordinary[j].MustParsedRequest().ServerReceivedTime,
			)
		})

		events := make([]*schema.Event, len(ordinary))
		for i, hit := range ordinary {
			events[i] = &schema.Event{
				BoundHit: hit,
				Metadata: make(map[string]any),
				Values:   make(map[string]any),
			}
		}
		if c.enrichAnnotations != nil && len(annotations) > 0 {
			c.enrichAnnotations(events, annotations)
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

func splitAnnotations(protosession []*hits.Hit) (ordinary, annotations []*hits.Hit) {
	ordinary = make([]*hits.Hit, 0, len(protosession))
	for _, hit := range protosession {
		if hits.IsAnnotation(hit) {
			annotations = append(annotations, hit)
		} else {
			ordinary = append(ordinary, hit)
		}
	}
	return ordinary, annotations
}

// NewDirectCloser creates a new protosessions.Closer that writes the session directly to
// warehouse.Driver, without intermediate queue (suitable only for single-tenant setup)
func NewDirectCloser(
	writer SessionWriter,
	failureSleepDuration time.Duration,
	options ...DirectCloserOption,
) *DirectCloser {
	c := &DirectCloser{
		failureSleepDuration: failureSleepDuration,
		writer:               writer,
	}
	for _, option := range options {
		option(c)
	}
	return c
}
