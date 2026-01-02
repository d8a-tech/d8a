// Package splitter provides session splitting conditions.
package splitter

import (
	"time"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

type nullableStringColumnValueChangedCondition struct {
	columnName       string
	splitCause       SplitCause
	emptyValueSplits bool
}

func (c *nullableStringColumnValueChangedCondition) ShouldSplit(
	ctx *Context,
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

// NewUTMCampaignCondition creates a new split condition that splits
// the session when the UTM campaign value changes.
func NewUTMCampaignCondition() Condition {
	return &nullableStringColumnValueChangedCondition{
		columnName:       columns.CoreInterfaces.EventUtmCampaign.Field.Name,
		splitCause:       SplitCauseUtmCampaignChange,
		emptyValueSplits: true,
	}
}

// NewUserIDCondition creates a new split condition that splits
// the session when the user id value changes.
func NewUserIDCondition() Condition {
	return &nullableStringColumnValueChangedCondition{
		columnName:       columns.CoreInterfaces.EventUserID.Field.Name,
		splitCause:       SplitCauseUserIDChange,
		emptyValueSplits: false,
	}
}

type maxXEventsCondition struct {
	maxXEvents int
}

func (c *maxXEventsCondition) ShouldSplit(
	ctx *Context,
	_ *schema.Event,
) (SplitCause, bool) {
	if ctx.EventCount >= c.maxXEvents {
		return SplitCauseMaxXEvents, true
	}
	return SplitCauseNone, false
}

// NewMaxXEventsCondition creates a new split condition that splits
// the session when the number of events exceeds the maximum number of events.
func NewMaxXEventsCondition(maxXEvents int) Condition {
	return &maxXEventsCondition{
		maxXEvents: maxXEvents,
	}
}

type timeSinceFirstEventCondition struct {
	timeSinceFirstEvent time.Duration
}

func (c *timeSinceFirstEventCondition) ShouldSplit(
	ctx *Context,
	current *schema.Event,
) (SplitCause, bool) {
	if ctx.FirstEvent == nil {
		return SplitCauseNone, false
	}
	if current.BoundHit.MustParsedRequest().ServerReceivedTime.
		Sub(ctx.FirstEvent.BoundHit.MustParsedRequest().ServerReceivedTime) >= c.timeSinceFirstEvent {
		return SplitCauseTimeSinceFirstEvent, true
	}
	return SplitCauseNone, false
}

// NewTimeSinceFirstEventCondition creates a new split condition that splits
// the session when the time since the first event exceeds the maximum time.
func NewTimeSinceFirstEventCondition(timeSinceFirstEvent time.Duration) Condition {
	return &timeSinceFirstEventCondition{
		timeSinceFirstEvent: timeSinceFirstEvent,
	}
}
