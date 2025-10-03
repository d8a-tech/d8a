package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// SSESessionHitNumber is a session-scoped event column that writes the
// index of the event within the session
var SSESessionHitNumber = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSESessionHitNumber.ID,
	columns.CoreInterfaces.SSESessionHitNumber.Field,
	func(e *schema.Event, s *schema.Session) (any, error) {
		for i, candidate := range s.Events {
			if candidate == e {
				return int64(i), nil
			}
		}
		return nil, nil
	},
	columns.WithSessionScopedEventColumnRequired(false),
)

// SSESessionPageNumber is a session-scoped event column that tells of which
// page counting from beginning of the session the event is on
var SSESessionPageNumber = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSESessionPageNumber.ID,
	columns.CoreInterfaces.SSESessionPageNumber.Field,
	func(e *schema.Event, s *schema.Session) (any, error) {
		var currentPageNumber int64 = 0
		var currentPage string = s.Events[0].Values[columns.CoreInterfaces.EventPageLocation.Field.Name].(string)
		for _, candidate := range s.Events {
			if currentPage != candidate.Values[columns.CoreInterfaces.EventPageLocation.Field.Name].(string) {
				currentPageNumber++
				currentPage = candidate.Values[columns.CoreInterfaces.EventPageLocation.Field.Name].(string)
			}
			if candidate == e {
				return currentPageNumber, nil
			}
		}
		return nil, nil
	},
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageLocation.Version,
		},
	),
)
