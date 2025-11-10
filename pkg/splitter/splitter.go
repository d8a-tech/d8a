// Package splitter provides session splitting functionality.
package splitter

import (
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SplitCause indicates the reason why a session was split.
type SplitCause string

const (
	// SplitCauseNone indicates no split occurred.
	SplitCauseNone SplitCause = ""
	// SplitCauseUtmCampaignChange indicates split due to UTM campaign change.
	SplitCauseUtmCampaignChange SplitCause = "utm_campaign_change"
	// SplitCauseUserIDChange indicates split due to user ID change.
	SplitCauseUserIDChange SplitCause = "user_id_change"
	// SplitCauseMaxXEvents indicates split due to maximum number of events.
	SplitCauseMaxXEvents SplitCause = "max_x_events"
	// SplitCauseTimeSinceFirstEvent indicates split due to time since first event.
	SplitCauseTimeSinceFirstEvent SplitCause = "time_since_first_event"
)

// SessionSplitter splits a session into multiple sessions based on conditions.
type SessionSplitter interface {
	Split(*schema.Session) ([]*schema.Session, error)
}

// SplitContext holds state accumulated while processing events.
// Conditions can read and update this context to maintain state efficiently.
type SplitContext struct {
	// EventCount is the number of events processed so far in the current session.
	EventCount int
	// FirstEvent is the timestamp of the first event in the current session.
	FirstEvent *schema.Event
	// ColumnValues stores the last seen value for each column name.
	// Conditions can use this to track value changes without re-scanning all events.
	ColumnValues map[string]interface{}
}

// NewSplitContext creates a new context for session splitting.
func NewSplitContext() *SplitContext {
	return &SplitContext{
		EventCount:   0,
		ColumnValues: make(map[string]interface{}),
	}
}

// SplitCondition determines if a session should be split at a given event.
type SplitCondition interface {
	ShouldSplit(ctx *SplitContext, current *schema.Event) (SplitCause, bool)
}

type splitterImpl struct {
	conditions []SplitCondition
}

func (s *splitterImpl) Split(session *schema.Session) ([]*schema.Session, error) {
	if len(session.Events) == 0 {
		return []*schema.Session{session}, nil
	}

	endSessions := make([]*schema.Session, 0)
	lastSplitIndex := 0
	ctx := NewSplitContext()

	for i, event := range session.Events {
		if i == 0 {
			s.initializeContext(ctx, event)
			continue
		}

		shouldSplit := false
		var splitCause SplitCause

		for _, condition := range s.conditions {
			cause, split := condition.ShouldSplit(ctx, event)
			if split {
				shouldSplit = true
				splitCause = cause
				break
			}
		}

		if shouldSplit {
			newSession := schema.NewSession(session.Events[lastSplitIndex:i])
			endSessions = append(endSessions, newSession)
			event.Metadata["session_split_cause"] = splitCause
			lastSplitIndex = i
			ctx = NewSplitContext()
			s.initializeContext(ctx, event)
		} else {
			ctx.EventCount++
		}
	}
	endSessions = append(endSessions, schema.NewSession(session.Events[lastSplitIndex:]))
	return endSessions, nil
}

// initializeContext sets up the context with the first event of a session.
func (s *splitterImpl) initializeContext(ctx *SplitContext, event *schema.Event) {
	ctx.FirstEvent = event
	ctx.EventCount = 1
	// Process event through conditions to initialize their state
	for _, condition := range s.conditions {
		condition.ShouldSplit(ctx, event)
	}
}

// NewNoop creates a new session splitter that does not split the session.
func NewNoop() SessionSplitter {
	return &splitterImpl{
		conditions: []SplitCondition{},
	}
}

// New creates a new session splitter with the given conditions.
func New(conditions ...SplitCondition) SessionSplitter {
	return &splitterImpl{
		conditions: conditions,
	}
}

// Registry provides session splitters for properties.
type Registry interface {
	Splitter(propertyID string) (SessionSplitter, error)
}

type fromPropertySettingsSplitterRegistry struct {
	propertySettingsRegistry properties.SettingsRegistry
}

func (r *fromPropertySettingsSplitterRegistry) Splitter(propertyID string) (SessionSplitter, error) {
	propertySettings, err := r.propertySettingsRegistry.GetByPropertyID(propertyID)
	if err != nil {
		return nil, err
	}
	conditions := []SplitCondition{
		NewTimeSinceFirstEventSplitCondition(propertySettings.SplitByTimeSinceFirstEvent),
		NewMaxXEventsSplitCondition(propertySettings.SplitByMaxEvents),
	}
	if propertySettings.SplitByUserID {
		conditions = append(conditions, NewUserIDSplitCondition())
	}
	if propertySettings.SplitByCampaign {
		conditions = append(conditions, NewUTMCampaignSplitCondition())
	}

	return New(conditions...), nil
}

// NewFromPropertySettingsSplitterRegistry creates a registry that builds splitters from property settings.
func NewFromPropertySettingsSplitterRegistry(propertySettingsRegistry properties.SettingsRegistry) Registry {
	return &fromPropertySettingsSplitterRegistry{
		propertySettingsRegistry: propertySettingsRegistry,
	}
}

type staticSplitterRegistry struct {
	splitter SessionSplitter
}

func (r *staticSplitterRegistry) Splitter(_ string) (SessionSplitter, error) {
	return r.splitter, nil
}

// NewStaticSplitterRegistry always returns the same splitter.
func NewStaticSplitterRegistry(splitter SessionSplitter) Registry {
	return &staticSplitterRegistry{splitter: splitter}
}
