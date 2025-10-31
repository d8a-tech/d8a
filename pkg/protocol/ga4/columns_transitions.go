package ga4

import (
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// EventNameIs returns a function that checks if an event has the specified event name.
func EventNameIs(targetEventName string) func(event *schema.Event) bool {
	return func(event *schema.Event) bool {
		eventName, ok := event.Values[columns.CoreInterfaces.EventName.Field.Name]
		if !ok {
			return false
		}
		eventNameStr, ok := eventName.(string)
		if !ok {
			return false
		}
		return eventNameStr == targetEventName
	}
}

// NewValueTransitionColumn creates a session-scoped event column that calculates
// consecutive values based on a chain of events.
func NewValueTransitionColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	chainFieldName string,
	nextHop func(event *schema.Event) bool,
	transformer func([]string),
	options ...columns.SessionScopedEventColumnOptions,
) schema.SessionScopedEventColumn {
	if transformer == nil {
		transformer = func(_ []string) {}
	}
	cacheKey := fmt.Sprintf("cache-%s-%s-%s", id, field.Name, chainFieldName)
	return columns.NewSimpleSessionScopedEventColumn(
		id,
		field,
		func(s *schema.Session, i int) (any, error) {
			var finalChain []string
			finalChainAny, ok := s.Metadata[cacheKey]
			if ok {
				finalChain, ok = finalChainAny.([]string)
				if ok {
					return finalChain[i], nil
				}
			}
			consecutiveValuesChain := make([]string, len(s.Events))
			for idx := range consecutiveValuesChain {
				hasNextHop := nextHop(s.Events[idx])
				switch {
				case hasNextHop:
					value, ok := s.Events[idx].Values[chainFieldName]
					if !ok {
						consecutiveValuesChain[idx] = ""
					}
					valueStr, ok := value.(string)
					if !ok {
						valueStr = ""
					}
					consecutiveValuesChain[idx] = valueStr
				case !hasNextHop && idx != 0:
					consecutiveValuesChain[idx] = consecutiveValuesChain[idx-1]
				default:
					consecutiveValuesChain[idx] = ""
				}
			}
			transformer(consecutiveValuesChain)
			finalChain = make([]string, len(consecutiveValuesChain))
			currValue := ""
			previousValue := ""
			for idx, processedValue := range consecutiveValuesChain {
				if processedValue == currValue {
					finalChain[idx] = previousValue
					continue
				}
				finalChain[idx] = currValue
				previousValue = currValue
				currValue = processedValue
			}
			transformer(finalChain)
			s.Metadata[cacheKey] = finalChain

			return finalChain[i], nil
		},
		options...,
	)
}

var eventPreviousPageLocationColumn = NewValueTransitionColumn(
	ProtocolInterfaces.EventPreviousPageLocation.ID,
	ProtocolInterfaces.EventPreviousPageLocation.Field,
	columns.CoreInterfaces.EventPageLocation.Field.Name,
	EventNameIs("page_view"),
	nil,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageLocation.Version,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Previous Page Location",
		"The URL of the previous page viewed in the session before the current page. "+
			"Only populated when a page transition is detected. "+
			"Returns nil for the first page or when no page change has occurred.",
	),
)

var eventNextPageLocationColumn = NewValueTransitionColumn(
	ProtocolInterfaces.EventNextPageLocation.ID,
	ProtocolInterfaces.EventNextPageLocation.Field,
	columns.CoreInterfaces.EventPageLocation.Field.Name,
	EventNameIs("page_view"),
	slices.Reverse,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageLocation.Version,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Next Page Location",
		"The URL of the next page viewed in the session after the current page. "+
			"Only populated when a page transition is detected. "+
			"Returns nil for the first page or when no page change has occurred.",
	),
)

var eventPreviousPageTitleColumn = NewValueTransitionColumn(
	ProtocolInterfaces.EventPreviousPageTitle.ID,
	ProtocolInterfaces.EventPreviousPageTitle.Field,
	columns.CoreInterfaces.EventPageTitle.Field.Name,
	EventNameIs("page_view"),
	nil,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageTitle.Version,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Previous Page Title",
		"The title of the previous page viewed in the session before the current page. "+
			"Only populated when a page transition is detected. "+
			"Returns nil for the first page or when no page change has occurred.",
	),
)

var eventNextPageTitleColumn = NewValueTransitionColumn(
	ProtocolInterfaces.EventNextPageTitle.ID,
	ProtocolInterfaces.EventNextPageTitle.Field,
	columns.CoreInterfaces.EventPageTitle.Field.Name, EventNameIs("page_view"),
	slices.Reverse,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventPageTitle.Version,
		},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Next Page Title",
		"The title of the next page viewed in the session after the current page. "+
			"Only populated when a page transition is detected. "+
			"Returns nil for the first page or when no page change has occurred.",
	),
)
