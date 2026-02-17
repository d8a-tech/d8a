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
	SplitCauseUtmCampaignChange SplitCause = "utm_campaign_changed"
	// SplitCauseUserIDChange indicates split due to user ID change.
	SplitCauseUserIDChange SplitCause = "user_id_changed"
	// SplitCauseMaxXEvents indicates split due to maximum number of events.
	SplitCauseMaxXEvents SplitCause = "max_events_reached"
	// SplitCauseTimeSinceFirstEvent indicates split due to time since first event.
	SplitCauseTimeSinceFirstEvent SplitCause = "max_time_since_first_event_reached"
)

// AllCauses is a list of all possible split causes, usable for documentation.
var AllCauses = []SplitCause{
	SplitCauseUtmCampaignChange,
	SplitCauseUserIDChange,
	SplitCauseMaxXEvents,
	SplitCauseTimeSinceFirstEvent,
}

// SessionModifier splits a session into multiple sessions based on conditions.
type SessionModifier interface {
	Split(*schema.Session) ([]*schema.Session, error)
}

// Context holds state accumulated while processing events.
// Conditions can read and update this context to maintain state efficiently.
type Context struct {
	// EventCount is the number of events processed so far in the current session.
	EventCount int
	// FirstEvent is the timestamp of the first event in the current session.
	FirstEvent *schema.Event
	// ColumnValues stores the last seen value for each column name.
	// Conditions can use this to track value changes without re-scanning all events.
	ColumnValues map[string]any
}

// NewSplitContext creates a new context for session splitting.
func NewSplitContext() *Context {
	return &Context{
		EventCount:   0,
		ColumnValues: make(map[string]any),
	}
}

// Condition determines if a session should be split at a given event.
type Condition interface {
	ShouldSplit(ctx *Context, current *schema.Event) (SplitCause, bool)
}

type splitterImpl struct {
	conditions []Condition
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
func (s *splitterImpl) initializeContext(ctx *Context, event *schema.Event) {
	ctx.FirstEvent = event
	ctx.EventCount = 1
	// Process event through conditions to initialize their state
	for _, condition := range s.conditions {
		condition.ShouldSplit(ctx, event)
	}
}

// NewNoop creates a new session splitter that does not split the session.
func NewNoop() SessionModifier {
	return &splitterImpl{
		conditions: []Condition{},
	}
}

// New creates a new session splitter with the given conditions.
func New(conditions ...Condition) SessionModifier {
	return &splitterImpl{
		conditions: conditions,
	}
}

// Registry provides session splitters for properties.
type Registry interface {
	Splitter(propertyID string) (SessionModifier, error)
}

type fromPropertySettingsRegistry struct {
	psr properties.SettingsRegistry
}

func (r *fromPropertySettingsRegistry) Splitter(propertyID string) (SessionModifier, error) {
	settings, err := r.psr.GetByPropertyID(propertyID)
	if err != nil {
		return nil, err
	}
	conditions := []Condition{
		NewTimeSinceFirstEventCondition(settings.SplitByTimeSinceFirstEvent),
		NewMaxXEventsCondition(settings.SplitByMaxEvents),
	}
	if settings.SplitByUserID {
		conditions = append(conditions, NewUserIDCondition())
	}
	if settings.SplitByCampaign {
		conditions = append(conditions, NewUTMCampaignCondition())
	}

	return New(conditions...), nil
}

// NewFromPropertySettingsRegistry creates a registry that builds splitters from property settings.
func NewFromPropertySettingsRegistry(psr properties.SettingsRegistry) Registry {
	return &fromPropertySettingsRegistry{
		psr: psr,
	}
}

type staticRegistry struct {
	splitter SessionModifier
}

func (r *staticRegistry) Splitter(_ string) (SessionModifier, error) {
	return r.splitter, nil
}

// NewStaticRegistry always returns the same splitter.
func NewStaticRegistry(splitter SessionModifier) Registry {
	return &staticRegistry{splitter: splitter}
}

// MultiModifier chains multiple SessionModifiers, feeding the output
// sessions of one into the next.
type MultiModifier struct {
	modifiers []SessionModifier
}

// NewMultiModifier creates a new multi-modifier that chains multiple SessionModifiers.
func NewMultiModifier(modifiers ...SessionModifier) SessionModifier {
	return &MultiModifier{modifiers: modifiers}
}

func (m *MultiModifier) Split(session *schema.Session) ([]*schema.Session, error) {
	sessions := []*schema.Session{session}
	for _, modifier := range m.modifiers {
		var next []*schema.Session
		for _, s := range sessions {
			result, err := modifier.Split(s)
			if err != nil {
				return nil, err
			}
			next = append(next, result...)
		}
		sessions = next
	}
	return sessions, nil
}

// chainedRegistry wraps a base Registry and prepends an additional SessionModifier.
type chainedRegistry struct {
	base    Registry
	prepend SessionModifier
}

// NewChainedRegistry creates a registry that prepends a modifier to another registry's modifiers.
func NewChainedRegistry(base Registry, prepend SessionModifier) Registry {
	return &chainedRegistry{base: base, prepend: prepend}
}

func (r *chainedRegistry) Splitter(propertyID string) (SessionModifier, error) {
	baseSplitter, err := r.base.Splitter(propertyID)
	if err != nil {
		return nil, err
	}
	return NewMultiModifier(r.prepend, baseSplitter), nil
}
