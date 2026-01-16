// Package sessions provides session management functionality for the tracking system.
package sessions

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// SessionWriter defines the interface for writing sessions
type SessionWriter interface {
	Write(sessions ...*schema.Session) error
}

// GroupKeyFunc maps a property ID to a grouping key.
// Properties with the same group key will be written sequentially within that group.
// Default behavior: groupKey == propertyID (one group per property).
type GroupKeyFunc func(propertyID string) (string, error)

// DirectCloserOption configures DirectCloser behavior.
type DirectCloserOption func(*DirectCloser)

// WithGroupKeyFunc sets a custom grouping function.
// Default: groups by propertyID (each property gets its own group).
func WithGroupKeyFunc(fn GroupKeyFunc) DirectCloserOption {
	return func(dc *DirectCloser) {
		dc.groupKeyFunc = fn
	}
}

// WithMaxConcurrentGroups sets the maximum number of groups that can be written concurrently.
// Default: 1 (sequential writes, safest for shared warehouses).
func WithMaxConcurrentGroups(n int) DirectCloserOption {
	return func(dc *DirectCloser) {
		dc.maxConcurrentGroups = n
	}
}

type DirectCloser struct {
	failureSleepDuration time.Duration
	writer               SessionWriter
	groupKeyFunc         GroupKeyFunc
	maxConcurrentGroups  int
}

// Close implements protosessions.Closer
func (c *DirectCloser) Close(protosessions [][]*hits.Hit) error {
	sessions := make([]*schema.Session, 0, len(protosessions))

	for _, protosession := range protosessions {
		if len(protosession) == 0 {
			continue
		}

		// Validate all hits in a proto-session have the same PropertyID
		firstPropertyID := protosession[0].PropertyID
		for _, hit := range protosession {
			if hit.PropertyID != firstPropertyID {
				return fmt.Errorf("proto-session contains mixed property IDs: found %q and %q", firstPropertyID, hit.PropertyID)
			}
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

	// Group sessions by group key, then sub-group by PropertyID within each group
	groupedSessions, err := c.groupSessions(sessions)
	if err != nil {
		return fmt.Errorf("failed to group sessions: %w", err)
	}

	// Write each group, with optional concurrency control
	return c.writeGroupedSessions(groupedSessions)
}

// groupSessions groups sessions by group key, then sub-groups by PropertyID within each group.
// Returns: map[groupKey]map[propertyID][]*schema.Session
func (c *DirectCloser) groupSessions(sessions []*schema.Session) (map[string]map[string][]*schema.Session, error) {
	grouped := make(map[string]map[string][]*schema.Session)

	for _, session := range sessions {
		// Get group key for this property
		groupKey, err := c.groupKeyFunc(session.PropertyID)
		if err != nil {
			return nil, fmt.Errorf("failed to get group key for property %q: %w", session.PropertyID, err)
		}

		// Initialize nested map if needed
		if grouped[groupKey] == nil {
			grouped[groupKey] = make(map[string][]*schema.Session)
		}

		// Sub-group by PropertyID within the group
		grouped[groupKey][session.PropertyID] = append(grouped[groupKey][session.PropertyID], session)
	}

	return grouped, nil
}

// writeGroupedSessions writes sessions grouped by group key, with optional concurrency control.
// Within each group, writes are sequential per PropertyID to maintain the invariant.
func (c *DirectCloser) writeGroupedSessions(groupedSessions map[string]map[string][]*schema.Session) error {
	if len(groupedSessions) == 0 {
		return nil
	}

	// If maxConcurrentGroups is 1 or less, write sequentially
	if c.maxConcurrentGroups <= 1 {
		return c.writeGroupedSessionsSequential(groupedSessions)
	}

	// Otherwise, write groups concurrently with limit
	return c.writeGroupedSessionsConcurrent(groupedSessions)
}

// writeGroupedSessionsSequential writes all groups sequentially.
func (c *DirectCloser) writeGroupedSessionsSequential(groupedSessions map[string]map[string][]*schema.Session) error {
	for groupKey, propertyGroups := range groupedSessions {
		if err := c.writePropertyGroups(groupKey, propertyGroups); err != nil {
			return err
		}
	}
	return nil
}

// writeGroupedSessionsConcurrent writes groups concurrently with a limit.
func (c *DirectCloser) writeGroupedSessionsConcurrent(groupedSessions map[string]map[string][]*schema.Session) error {
	g, _ := errgroup.WithContext(context.TODO())
	g.SetLimit(c.maxConcurrentGroups)

	for groupKey, propertyGroups := range groupedSessions {
		groupKey := groupKey             // capture for goroutine
		propertyGroups := propertyGroups // capture for goroutine
		g.Go(func() error {
			return c.writePropertyGroups(groupKey, propertyGroups)
		})
	}

	return g.Wait()
}

// writePropertyGroups writes all property groups within a single group key sequentially.
// This ensures we never call writer.Write with mixed PropertyIDs.
func (c *DirectCloser) writePropertyGroups(groupKey string, propertyGroups map[string][]*schema.Session) error {
	for propertyID, sessions := range propertyGroups {
		if err := c.writer.Write(sessions...); err != nil {
			logrus.Errorf(
				"failed to write sessions for group %q property %q: %v, adding Sleep to avoid spamming the warehouse",
				groupKey, propertyID, err,
			)
			time.Sleep(c.failureSleepDuration)
			return err
		}
	}
	return nil
}

// NewDirectCloser creates a new protosessions.Closer that writes the session directly to
// warehouse.Driver, without intermediate queue (suitable only for single-tenant setup).
// Default behavior: groups by PropertyID (one group per property, sequential writes).
func NewDirectCloser(
	writer SessionWriter,
	failureSleepDuration time.Duration,
	opts ...DirectCloserOption,
) *DirectCloser {
	dc := &DirectCloser{
		failureSleepDuration: failureSleepDuration,
		writer:               writer,
		maxConcurrentGroups:  1, // Default: sequential writes
		groupKeyFunc: func(propertyID string) (string, error) {
			// Default: group by PropertyID
			return propertyID, nil
		},
	}

	for _, opt := range opts {
		opt(dc)
	}

	return dc
}
