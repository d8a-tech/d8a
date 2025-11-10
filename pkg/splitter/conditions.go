// Package splitter provides session splitting conditions.
package splitter

import (
	"time"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

type nullableStringColumnValueChangedSplitCondition struct {
	columnName       string
	splitCause       SplitCause
	emptyValueSplits bool
}

func (c *nullableStringColumnValueChangedSplitCondition) ShouldSplit(
	ctx *SplitContext,
	current *schema.Event,
) (SplitCause, bool) {
	// Get current value
	curValAny, ok := current.Values[c.columnName]
	if !ok {
		return SplitCauseNone, false
	}
	curValStr, ok := curValAny.(string)
	if !ok {
		return SplitCauseNone, false
	}

	// Check if empty values should trigger splits
	if !c.emptyValueSplits && curValStr == "" {
		return SplitCauseNone, false
	}

	// Get last value from context
	lastValAny, hasLastVal := ctx.ColumnValues[c.columnName]
	lastVal := ""
	if hasLastVal {
		if lastValStr, ok := lastValAny.(string); ok {
			lastVal = lastValStr
		}
	}

	// Update context with current value
	ctx.ColumnValues[c.columnName] = curValStr

	// Check if value changed
	if hasLastVal && curValStr != lastVal {
		return c.splitCause, true
	}

	return SplitCauseNone, false
}

// NewUTMCampaignSplitCondition creates a new split condition that splits
// the session when the UTM campaign value changes.
func NewUTMCampaignSplitCondition() SplitCondition {
	return &nullableStringColumnValueChangedSplitCondition{
		columnName:       columns.CoreInterfaces.EventUtmCampaign.Field.Name,
		splitCause:       SplitCauseUtmCampaignChange,
		emptyValueSplits: true,
	}
}

// NewUserIDSplitCondition creates a new split condition that splits
// the session when the user id value changes.
func NewUserIDSplitCondition() SplitCondition {
	return &nullableStringColumnValueChangedSplitCondition{
		columnName:       columns.CoreInterfaces.EventUserID.Field.Name,
		splitCause:       SplitCauseUserIDChange,
		emptyValueSplits: false,
	}
}

type maxXEventsSplitCondition struct {
	maxXEvents int
}

func (c *maxXEventsSplitCondition) ShouldSplit(
	ctx *SplitContext,
	_ *schema.Event,
) (SplitCause, bool) {
	if ctx.EventCount >= c.maxXEvents {
		return SplitCauseMaxXEvents, true
	}
	return SplitCauseNone, false
}

// NewMaxXEventsSplitCondition creates a new split condition that splits
// the session when the number of events exceeds the maximum number of events.
func NewMaxXEventsSplitCondition(maxXEvents int) SplitCondition {
	return &maxXEventsSplitCondition{
		maxXEvents: maxXEvents,
	}
}

type timeSinceFirstEventSplitCondition struct {
	timeSinceFirstEvent time.Duration
}

func (c *timeSinceFirstEventSplitCondition) ShouldSplit(
	ctx *SplitContext,
	current *schema.Event,
) (SplitCause, bool) {
	if ctx.FirstEvent == nil {
		return SplitCauseNone, false
	}
	if current.BoundHit.ServerReceivedTime.Sub(ctx.FirstEvent.BoundHit.ServerReceivedTime) >= c.timeSinceFirstEvent {
		return SplitCauseTimeSinceFirstEvent, true
	}
	return SplitCauseNone, false
}

// NewTimeSinceFirstEventSplitCondition creates a new split condition that splits
// the session when the time since the first event exceeds the maximum time.
func NewTimeSinceFirstEventSplitCondition(timeSinceFirstEvent time.Duration) SplitCondition {
	return &timeSinceFirstEventSplitCondition{
		timeSinceFirstEvent: timeSinceFirstEvent,
	}
}
