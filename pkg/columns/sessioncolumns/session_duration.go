// Package sessioncolumns provides column implementations for session data tracking.
package sessioncolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// DurationColumn is the column for the duration of a session
var DurationColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionDuration.ID,
	columns.CoreInterfaces.SessionDuration.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		lastEventTime, ok := session.Values[columns.CoreInterfaces.SessionLastEventTime.Field.Name]
		if !ok {
			return nil, schema.NewBrokenSessionError("session last event time not found")
		}
		firstEventTime, ok := session.Values[columns.CoreInterfaces.SessionFirstEventTime.Field.Name]
		if !ok {
			return nil, schema.NewBrokenSessionError("session first event time not found")
		}
		lastEventTimeInt, ok := lastEventTime.(int64)
		if !ok {
			return nil, schema.NewBrokenSessionError("session last event time is not an int64")
		}
		firstEventTimeInt, ok := firstEventTime.(int64)
		if !ok {
			return nil, schema.NewBrokenSessionError("session first event time is not an int64")
		}
		if lastEventTimeInt < firstEventTimeInt {
			return nil, schema.NewBrokenSessionError("session last event time is earlier than session first event time")
		}
		return lastEventTimeInt - firstEventTimeInt, nil
	}, columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.SessionLastEventTime.ID,
		}, schema.DependsOnEntry{
			Interface: columns.CoreInterfaces.SessionFirstEventTime.ID,
		}),
	columns.WithSessionColumnDocs(
		"Session Duration",
		"The duration of the session in seconds, calculated as the difference between the last event time and the first event time. Zero for single-event sessions.", // nolint:lll // it's a description
	),
)
