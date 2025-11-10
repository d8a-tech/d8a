// Package schema provides the core data models and types for the tracking system.
package schema

import "github.com/d8a-tech/d8a/pkg/hits"

// Event represents a test event with bound hit and all hits.
type Event struct {
	BoundHit *hits.Hit
	Metadata map[string]any
	Values   map[string]any
}

// NewEvent creates a new event with a bound hit and empty metadata and values.
func NewEvent(boundHit *hits.Hit) *Event {
	return &Event{
		BoundHit: boundHit,
		Metadata: make(map[string]any),
		Values:   make(map[string]any),
	}
}

// WithValueKey sets a value for a key in the event values.
func (e *Event) WithValueKey(key string, value any) *Event {
	e.Values[key] = value
	return e
}

// Session represents a session with events and values.
type Session struct {
	PropertyID string
	Metadata   map[string]any

	Events []*Event
	Values map[string]any
}

// NewSession creates a new session from a slice of events.
func NewSession(events []*Event) *Session {
	propertyID := ""
	if len(events) > 0 && events[0].BoundHit != nil {
		propertyID = events[0].BoundHit.PropertyID
	}
	return &Session{
		PropertyID: propertyID,
		Metadata:   make(map[string]any),
		Events:     events,
		Values:     make(map[string]any),
	}
}
