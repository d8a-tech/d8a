// Package schema provides the core data models and types for the tracking system.
package schema

import "github.com/d8a-tech/d8a/pkg/hits"

// Event represents a test event with bound hit and all hits.
type Event struct {
	BoundHit *hits.Hit
	Metadata map[string]any
	Values   map[string]any
}

// Session represents a session with events and values.
type Session struct {
	PropertyID string
	Metadata   map[string]any

	Events []*Event
	Values map[string]any
}
