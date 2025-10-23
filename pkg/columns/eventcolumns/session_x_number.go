// Package eventcolumns provides event column implementations for session-scoped data.
package eventcolumns

import (
	"errors"

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
		return nil, errors.New("event not found in session")
	},
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDocs(
		"Session Hit Number",
		"The sequential number of this event within its session, starting from 0. This is the index position of the event in the chronological sequence of all events in the same session.", // nolint:lll // it's a description
	),
)

// SSESessionPageNumber is a session-scoped event column that tells of which
// page counting from beginning of the session the event is on
var SSESessionPageNumber = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSESessionPageNumber.ID,
	columns.CoreInterfaces.SSESessionPageNumber.Field,
	func(e *schema.Event, s *schema.Session) (any, error) {
		var currentPageNumber int64 = 0
		currentPageValue, ok := s.Events[0].Values[columns.CoreInterfaces.EventPageLocation.Field.Name].(string)
		if !ok {
			return nil, errors.New("invalid page location type")
		}
		var currentPage string = currentPageValue
		for _, candidate := range s.Events {
			candidatePageValue, ok := candidate.Values[columns.CoreInterfaces.EventPageLocation.Field.Name].(string)
			if !ok {
				return nil, errors.New("invalid page location type")
			}
			if currentPage != candidatePageValue {
				currentPageNumber++
				currentPage = candidatePageValue
			}
			if candidate == e {
				return currentPageNumber, nil
			}
		}
		return nil, errors.New("event not found in session")
	},
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageLocation.Version,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Session Page Number",
		"The sequential page number within the session, starting from 0. Increments when the page location changes. Tracks which page view in the session this event occurred on.", // nolint:lll // it's a description
	),
)
