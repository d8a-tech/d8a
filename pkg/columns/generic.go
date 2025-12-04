// Package columns provides generic column implementations for session data tracking.
package columns

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/sirupsen/logrus"
)

type simpleSessionColumn struct {
	id        schema.InterfaceID
	field     *arrow.Field
	docs      schema.Documentation
	dependsOn []schema.DependsOnEntry
	required  bool
	castFunc  func(any) (any, error)
	write     func(*simpleSessionColumn, *schema.Session) error
}

func (c *simpleSessionColumn) Docs() schema.Documentation {
	return c.docs
}

func (c *simpleSessionColumn) Implements() schema.Interface {
	return schema.Interface{
		ID:      c.id,
		Version: "1.0.0",
		Field:   c.field,
	}
}

func (c *simpleSessionColumn) Version() schema.Version {
	return "1.0.0"
}

func (c *simpleSessionColumn) Field() *arrow.Field {
	if c.required {
		c.field.Nullable = false
	}
	return c.field
}

func (c *simpleSessionColumn) DependsOn() []schema.DependsOnEntry {
	return c.dependsOn
}

func (c *simpleSessionColumn) Write(session *schema.Session) error {
	return c.write(c, session)
}

// NewSimpleSessionColumn creates a new simple session column with the given configuration
//
//nolint:dupl // false positive - .write function cannot be extracted due to type differences
func NewSimpleSessionColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	getValue func(*schema.Session) (any, error),
	options ...SessionColumnOptions,
) schema.SessionColumn {
	c := &simpleSessionColumn{
		id:    id,
		field: field,
		docs: schema.Documentation{
			ColumnName:  field.Name,
			Type:        field,
			InterfaceID: string(id),
		},
	}
	c.write = func(c *simpleSessionColumn, session *schema.Session) error {
		value, err := getValue(session)
		if err != nil {
			return err
		}
		casted, err := c.castFunc(value)
		if err != nil {
			return err
		}
		session.Values[field.Name] = casted
		return nil
	}
	for _, option := range options {
		option(c)
	}
	if c.castFunc == nil {
		c.castFunc = func(value any) (any, error) {
			return value, nil
		}
	}
	return c
}

// FromQueryParamSessionColumn creates a new session column from a query param
func FromQueryParamSessionColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	queryParam string,
	options ...SessionColumnOptions,
) schema.SessionColumn {
	return NewSimpleSessionColumn(id, field, func(session *schema.Session) (any, error) {
		if len(session.Events) == 0 {
			return nil, nil
		}
		lastEvent := session.Events[len(session.Events)-1]
		if lastEvent.BoundHit.QueryParams == nil {
			return nil, nil
		}
		value := lastEvent.BoundHit.QueryParams.Get(queryParam)
		return value, nil
	}, options...)
}

// NthEventMatchingPredicateValueColumn creates a session column that extracts a value from the nth event
// that matches the given predicate. This allows protocol-specific filtering (e.g., only page view events).
// Supports negative indices to count from the end (e.g., -1 for last matching event).
func NthEventMatchingPredicateValueColumn(
	columnID schema.InterfaceID,
	field *arrow.Field,
	n int,
	extractedField string,
	matches func(*schema.Event) bool,
	options ...SessionColumnOptions,
) schema.SessionColumn {
	return NewSimpleSessionColumn(
		columnID,
		field,
		func(session *schema.Session) (any, error) {
			matchingEvents := make([]*schema.Event, 0)
			for _, event := range session.Events {
				if matches(event) {
					matchingEvents = append(matchingEvents, event)
				}
			}
			if len(matchingEvents) == 0 {
				return nil, nil // nolint:nilnil // nil is valid for this column
			}
			index := n
			if index < 0 {
				index = len(matchingEvents) + index
			}

			if index < 0 || index >= len(matchingEvents) {
				return nil, nil // nolint:nilnil // nil is valid for this column
			}
			v, ok := matchingEvents[index].Values[extractedField]
			if !ok {
				return nil, nil // nolint:nilnil // nil is valid for this column
			}
			return v, nil
		},
		options...,
	)
}

// URLElementColumn creates a new event column from a URL element
func URLElementColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	getValue func(e *schema.Event, url *url.URL) (any, error),
	options ...EventColumnOptions,
) schema.EventColumn {
	options = append(options,
		WithEventColumnRequired(false),
		WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        CoreInterfaces.EventPageLocation.ID,
				GreaterOrEqualTo: CoreInterfaces.EventPageLocation.Version,
			},
		),
	)
	return NewSimpleEventColumn(
		id,
		field,
		func(e *schema.Event) (any, error) {
			pageLocation, ok := e.Values[CoreInterfaces.EventPageLocation.Field.Name]
			if !ok {
				return nil, nil
			}
			pageLocationStr, ok := pageLocation.(string)
			if !ok {
				return nil, nil
			}
			parsed, err := url.Parse(pageLocationStr)
			if err != nil {
				return nil, err
			}
			return getValue(e, parsed)
		},
		options...,
	)
}

// SessionColumnOptions configures a simple session column
type SessionColumnOptions func(*simpleSessionColumn)

// WithSessionColumnDependsOn sets the dependencies for a session column
func WithSessionColumnDependsOn(dependsOn ...schema.DependsOnEntry) SessionColumnOptions {
	return func(c *simpleSessionColumn) {
		c.dependsOn = dependsOn
	}
}

// WithSessionColumnRequired sets whether a session column is required
func WithSessionColumnRequired(required bool) SessionColumnOptions {
	return func(c *simpleSessionColumn) {
		c.required = required
	}
}

// WithSessionColumnCast sets the cast function for a session column
func WithSessionColumnCast(castFunc func(any) (any, error)) SessionColumnOptions {
	return func(c *simpleSessionColumn) {
		c.castFunc = castFunc
	}
}

// WithSessionColumnDocs sets the documentation for a session column
func WithSessionColumnDocs(displayName, description string) SessionColumnOptions {
	return func(c *simpleSessionColumn) {
		c.docs = defaultDocumentation(c.Implements(), displayName, description)
	}
}

type simpleEventColumn struct {
	id        schema.InterfaceID
	field     *arrow.Field
	docs      schema.Documentation
	dependsOn []schema.DependsOnEntry
	required  bool
	castFunc  func(any) (any, error)
	write     func(*simpleEventColumn, *schema.Event) error
}

func (c *simpleEventColumn) Docs() schema.Documentation {
	return c.docs
}

func (c *simpleEventColumn) Implements() schema.Interface {
	return schema.Interface{
		ID:      c.id,
		Version: "1.0.0",
		Field:   c.field,
	}
}

func (c *simpleEventColumn) Version() schema.Version {
	return "1.0.0"
}

func (c *simpleEventColumn) Field() *arrow.Field {
	if c.required {
		c.field.Nullable = false
	}
	return c.field
}

func (c *simpleEventColumn) DependsOn() []schema.DependsOnEntry {
	return c.dependsOn
}

func (c *simpleEventColumn) Write(event *schema.Event) error {
	return c.write(c, event)
}

// NewSimpleEventColumn creates a new simple event column with the given configuration
//
//nolint:dupl // false positive - .write function cannot be extracted due to type differences
func NewSimpleEventColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	getValue func(*schema.Event) (any, error),
	options ...EventColumnOptions,
) schema.EventColumn {
	c := &simpleEventColumn{
		id:    id,
		field: field,
		docs: schema.Documentation{
			ColumnName:  field.Name,
			Type:        field,
			InterfaceID: string(id),
		},
	}
	c.write = func(c *simpleEventColumn, event *schema.Event) error {
		value, err := getValue(event)
		if err != nil {
			return err
		}
		casted, err := c.castFunc(value)
		if err != nil {
			return err
		}
		event.Values[field.Name] = casted
		return nil
	}
	for _, option := range options {
		option(c)
	}
	if c.castFunc == nil {
		c.castFunc = func(value any) (any, error) {
			return value, nil
		}
	}
	return c
}

// FromQueryParamEventColumn creates a new event column from a query param
func FromQueryParamEventColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	queryParam string,
	options ...EventColumnOptions,
) schema.EventColumn {
	return NewSimpleEventColumn(id, field, func(event *schema.Event) (any, error) {
		if len(event.BoundHit.QueryParams) == 0 {
			return nil, nil
		}
		return event.BoundHit.QueryParams.Get(queryParam), nil
	}, options...)
}

// AlwaysNilEventColumn creates a new event column that always returns nil
func AlwaysNilEventColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	options ...EventColumnOptions,
) schema.EventColumn {
	return NewSimpleEventColumn(id, field, func(_ *schema.Event) (any, error) {
		return nil, nil // nolint:nilnil // nil is valid
	}, options...)
}

// FromPageURLEventColumn creates a new event column from a UTM tag
func FromPageURLEventColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	utmTag string,
	options ...EventColumnOptions,
) schema.EventColumn {
	options = append(options, WithEventColumnDependsOn(schema.DependsOnEntry{
		Interface:        CoreInterfaces.EventPageLocation.ID,
		GreaterOrEqualTo: "1.0.0",
	}))
	return NewSimpleEventColumn(id, field, func(event *schema.Event) (any, error) {
		pageLocation, ok := event.Values[CoreInterfaces.EventPageLocation.Field.Name]
		if !ok {
			return nil, nil
		}
		pageLocationStr, ok := pageLocation.(string)
		if !ok {
			return nil, nil
		}
		parsed, err := url.Parse(pageLocationStr)
		if err != nil {
			return nil, err
		}
		return parsed.Query().Get(utmTag), nil
	}, options...)
}

// EventColumnOptions configures a simple event column
type EventColumnOptions func(*simpleEventColumn)

// WithEventColumnDependsOn sets the dependencies for an event column
func WithEventColumnDependsOn(dependsOn ...schema.DependsOnEntry) EventColumnOptions {
	return func(c *simpleEventColumn) {
		c.dependsOn = dependsOn
	}
}

// WithEventColumnRequired sets whether an event column is required
func WithEventColumnRequired(required bool) EventColumnOptions {
	return func(c *simpleEventColumn) {
		c.required = required
	}
}

// WithEventColumnCast sets the cast function for an event column
func WithEventColumnCast(castFunc func(any) (any, error)) EventColumnOptions {
	return func(c *simpleEventColumn) {
		c.castFunc = castFunc
	}
}

// WithEventColumnDocs sets the documentation for an event column
func WithEventColumnDocs(displayName, description string) EventColumnOptions {
	return func(c *simpleEventColumn) {
		c.docs = defaultDocumentation(c.Implements(), displayName, description)
	}
}

// CastToInt64OrNil casts a value to int64 or returns nil if conversion fails or value is empty
func CastToInt64OrNil(columnID schema.InterfaceID) func(any) (any, error) {
	return func(value any) (any, error) {
		valueStr, ok := value.(string)
		if !ok {
			logrus.Debugf("CastToInt64OrNil: %s: value is not a string: %v", columnID, value)
			return nil, nil
		}
		if valueStr == "" {
			return nil, nil
		}
		casted, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			logrus.Debugf("CastToInt64OrNil: %s: value is not an int64: %v", columnID, value)
			return nil, nil
		}
		return casted, nil
	}
}

// CastToInt64OrZero casts a value to int64 or returns 0 if conversion fails or value is empty
func CastToInt64OrZero(columnID schema.InterfaceID) func(any) (any, error) {
	return func(value any) (any, error) {
		valueStr, ok := value.(string)
		if !ok {
			logrus.Debugf("CastToInt64OrZero: %s: value is not a string: %v", columnID, value)
			return 0, nil
		}
		if valueStr == "" {
			return 0, nil
		}
		casted, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			logrus.Debugf("CastToInt64OrZero: %s: value is not an int64: %v", columnID, value)
			return 0, nil
		}
		return casted, nil
	}
}

// CastToFloat64OrNil casts a value to float64 or returns nil if conversion fails or value is empty
func CastToFloat64OrNil(columnID schema.InterfaceID) func(any) (any, error) {
	return func(value any) (any, error) {
		valueStr, ok := value.(string)
		if !ok {
			logrus.Debugf("CastToFloat64OrNil: %s: value is not a string: %v", columnID, value)
			return nil, nil
		}
		if valueStr == "" {
			return nil, nil
		}
		casted, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			logrus.Debugf("CastToFloat64OrNil: %s: value is not a float64: %v", columnID, value)
			return nil, nil
		}
		return casted, nil
	}
}

// CastToBool casts a value to bool considering various truthy representations
func CastToBool(_ schema.InterfaceID) func(any) (any, error) {
	return func(value any) (any, error) {
		// Handle boolean values directly
		if boolVal, ok := value.(bool); ok {
			return boolVal, nil
		}

		// Handle string values
		valueStr, ok := value.(string)
		if !ok {
			return false, nil
		}

		// Use util.StrToBool for string conversion
		boolVal, err := util.StrToBool(valueStr)
		if err != nil {
			return false, fmt.Errorf("failed to cast %s to bool: %w", valueStr, err)
		}
		return boolVal, nil
	}
}

// StrErrIfEmpty casts a value to string or returns an error if conversion fails or value is empty
func StrErrIfEmpty(ifID schema.InterfaceID) func(any) (any, error) {
	return func(value any) (any, error) {
		_, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("%s: value is not a string: %v", ifID, value)
		}
		if value == "" {
			return nil, fmt.Errorf("%s: value is empty: %v", ifID, value)
		}
		return value, nil
	}
}

// CastToString casts a value to string or returns nil if conversion fails
func CastToString(_ schema.InterfaceID) func(any) (any, error) {
	return func(value any) (any, error) {
		valueStr, ok := value.(string)
		if !ok {
			return nil, errors.New("value is not a string")
		}
		return valueStr, nil
	}
}

// NilIfError returns nil if the error is not nil
func NilIfError(i func(any) (any, error)) func(any) (any, error) {
	return func(value any) (any, error) {
		value, err := i(value)
		if err != nil {
			return nil, nil
		}
		return value, nil
	}
}

// StrNilIfErrorOrEmpty returns nil if the error is not nil or the value is an empty string
func StrNilIfErrorOrEmpty(i func(any) (any, error)) func(any) (any, error) {
	return func(value any) (any, error) {
		value, err := i(value)
		if err != nil {
			return nil, err
		}
		valueStr, ok := value.(string)
		if !ok {
			return nil, errors.New("value is not a string")
		}
		if valueStr == "" {
			return nil, nil
		}
		return valueStr, nil
	}
}

type simpleSessionScopedEventColumn struct {
	id        schema.InterfaceID
	field     *arrow.Field
	docs      schema.Documentation
	dependsOn []schema.DependsOnEntry
	required  bool
	castFunc  func(any) (any, error)
	write     func(*simpleSessionScopedEventColumn, *schema.Session, int) error
}

func (c *simpleSessionScopedEventColumn) Docs() schema.Documentation {
	return c.docs
}

func (c *simpleSessionScopedEventColumn) Implements() schema.Interface {
	return schema.Interface{
		ID:      c.id,
		Version: "1.0.0",
		Field:   c.field,
	}
}

func (c *simpleSessionScopedEventColumn) DependsOn() []schema.DependsOnEntry {
	return c.dependsOn
}

func (c *simpleSessionScopedEventColumn) Write(session *schema.Session, i int) error {
	return c.write(c, session, i)
}

// SessionScopedEventColumnOptions configures a simple session-scoped event column
type SessionScopedEventColumnOptions func(*simpleSessionScopedEventColumn)

// WithSessionScopedEventColumnDependsOn sets the dependencies for a session-scoped event column
func WithSessionScopedEventColumnDependsOn(dependsOn ...schema.DependsOnEntry) SessionScopedEventColumnOptions {
	return func(c *simpleSessionScopedEventColumn) {
		c.dependsOn = dependsOn
	}
}

// WithSessionScopedEventColumnRequired sets whether a session-scoped event column is required
func WithSessionScopedEventColumnRequired(required bool) SessionScopedEventColumnOptions {
	return func(c *simpleSessionScopedEventColumn) {
		c.required = required
	}
}

// WithSessionScopedEventColumnCast sets the cast function for a session-scoped event column
func WithSessionScopedEventColumnCast(castFunc func(any) (any, error)) SessionScopedEventColumnOptions {
	return func(c *simpleSessionScopedEventColumn) {
		c.castFunc = castFunc
	}
}

// WithSessionScopedEventColumnDocs sets the documentation for a session-scoped event column
func WithSessionScopedEventColumnDocs(displayName, description string) SessionScopedEventColumnOptions {
	return func(c *simpleSessionScopedEventColumn) {
		c.docs = defaultDocumentation(c.Implements(), displayName, description)
	}
}

// NewSimpleSessionScopedEventColumn creates a new session-scoped event column with the given configuration
//
//nolint:dupl // similar structure to other simple column builders, but types differ
func NewSimpleSessionScopedEventColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	getValue func(*schema.Session, int) (any, error),
	options ...SessionScopedEventColumnOptions,
) schema.SessionScopedEventColumn {
	c := &simpleSessionScopedEventColumn{
		id:    id,
		field: field,
		docs: schema.Documentation{
			ColumnName:  field.Name,
			Type:        field,
			InterfaceID: string(id),
		},
	}
	c.write = func(c *simpleSessionScopedEventColumn, session *schema.Session, i int) error {
		value, err := getValue(session, i)
		if err != nil {
			return err
		}
		casted, err := c.castFunc(value)
		if err != nil {
			return err
		}
		session.Events[i].Values[field.Name] = casted
		return nil
	}
	for _, option := range options {
		option(c)
	}
	if c.castFunc == nil {
		c.castFunc = func(value any) (any, error) { return value, nil }
	}
	if c.required {
		field.Nullable = false
	}
	return c
}

func defaultDocumentation(intf schema.Interface, displayName, description string) schema.Documentation {
	return schema.Documentation{
		ColumnName:  intf.Field.Name,
		DisplayName: displayName,
		Description: description,
		Type:        intf.Field,
		InterfaceID: string(intf.ID),
	}
}

// TransitionAdvanceFunction allows setting constraints for TransitionColumns
type TransitionAdvanceFunction func(event *schema.Event) bool

// TransitionTransformerFunction allows transforming the consecutive values chain
type TransitionTransformerFunction func([]string)

// TransitionAdvanceWhenEventNameIs returns a function that checks if an event has the specified event name.
func TransitionAdvanceWhenEventNameIs(targetEventName string) func(event *schema.Event) bool {
	return func(event *schema.Event) bool {
		eventName, ok := event.Values[CoreInterfaces.EventName.Field.Name]
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

// TransitionDirection specifies the direction of the transition
type TransitionDirection bool

const (
	// TransitionDirectionForward specifies the forward direction of the transition
	TransitionDirectionForward TransitionDirection = false
	// TransitionDirectionBackward specifies the backward direction of the transition
	TransitionDirectionBackward TransitionDirection = true
)

// NewValueTransitionColumn creates a session-scoped event column that calculates
// values by looking to find the closest event where the value differs from
// the current one. For example, previous_page_url or previous_page_title: if multiple
// consecutive events have the same page URL/title, it returns the value from the closest
// previous event that had a different value, not the immediately previous event.
func NewValueTransitionColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	chainFieldName string,
	advance TransitionAdvanceFunction,
	direction TransitionDirection,
	options ...SessionScopedEventColumnOptions,
) schema.SessionScopedEventColumn {
	var transformer = func(_ []any) {}
	if direction == TransitionDirectionForward {
		transformer = slices.Reverse
	}
	cacheKey := fmt.Sprintf("cache-%s-%s-%s", id, field.Name, chainFieldName)
	return NewSimpleSessionScopedEventColumn(
		id,
		field,
		func(s *schema.Session, i int) (any, error) {
			var finalChain []any
			finalChainAny, ok := s.Metadata[cacheKey]
			if ok {
				finalChain, ok = finalChainAny.([]any)
				if ok {
					return finalChain[i], nil
				}
			}
			consecutiveValuesChain := make([]any, len(s.Events))
			for idx := range consecutiveValuesChain {
				hasNextHop := advance(s.Events[idx])
				switch {
				case hasNextHop:
					value, ok := s.Events[idx].Values[chainFieldName]
					if !ok {
						consecutiveValuesChain[idx] = nil
						continue
					}
					valueStr, ok := value.(string)
					if ok {
						consecutiveValuesChain[idx] = valueStr
					} else {
						consecutiveValuesChain[idx] = nil
					}
				case !hasNextHop && idx != 0:
					consecutiveValuesChain[idx] = consecutiveValuesChain[idx-1]
				default:
					consecutiveValuesChain[idx] = nil
				}
			}
			transformer(consecutiveValuesChain)
			finalChain = make([]any, len(consecutiveValuesChain))
			var currValue any
			var previousValue any
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

// NewFirstLastMatchingEventColumn creates a session-scoped event column that marks
// the first or last event matching a predicate with 1, and all other events with 0.
// This is useful for marking entry/exit pages, where only the first/last page_view
// in a session should be marked.
func NewFirstLastMatchingEventColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	matches TransitionAdvanceFunction,
	isFirst bool,
	options ...SessionScopedEventColumnOptions,
) schema.SessionScopedEventColumn {
	cacheKey := fmt.Sprintf("cache-firstlast-%s-%v", id, isFirst)
	return NewSimpleSessionScopedEventColumn(
		id,
		field,
		func(s *schema.Session, i int) (any, error) {
			var result []int64
			resultAny, ok := s.Metadata[cacheKey]
			if ok {
				result, ok = resultAny.([]int64)
				if ok {
					return result[i], nil
				}
			}

			// Find all matching event indices
			matchingIndices := make([]int, 0)
			for idx, event := range s.Events {
				if matches(event) {
					matchingIndices = append(matchingIndices, idx)
				}
			}

			// Initialize result array with zeros
			result = make([]int64, len(s.Events))
			for idx := range result {
				result[idx] = 0
			}

			// Mark first or last matching event
			if len(matchingIndices) > 0 {
				if isFirst {
					result[matchingIndices[0]] = 1
				} else {
					result[matchingIndices[len(matchingIndices)-1]] = 1
				}
			}

			s.Metadata[cacheKey] = result
			return result[i], nil
		},
		options...,
	)
}

// TotalEventsOfGivenNameColumn creates a session column that counts the total number
// of events with the given event names.
func TotalEventsOfGivenNameColumn(
	columnID schema.InterfaceID,
	field *arrow.Field,
	eventNames []string,
	options ...SessionColumnOptions,
) schema.SessionColumn {
	options = append(options, WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	))
	return NewSimpleSessionColumn(
		columnID,
		field,
		func(session *schema.Session) (any, error) {
			totalEvents := 0
			for _, event := range session.Events {
				valueAsString, ok := event.Values[CoreInterfaces.EventName.Field.Name].(string)
				if !ok {
					continue
				}
				if slices.Contains(eventNames, valueAsString) {
					totalEvents++
				}
			}
			return totalEvents, nil
		},
		options...,
	)
}

// UniqueEventsOfGivenNameColumn creates a session column that counts the unique number
// of events. Deduplication strategy: Events are considered unique for unique combinations of event name
// and dependent column values.
func UniqueEventsOfGivenNameColumn(
	columnID schema.InterfaceID,
	field *arrow.Field,
	eventNames []string,
	dependentColumns []*arrow.Field,
	options ...SessionColumnOptions,
) schema.SessionColumn {
	options = append(options, WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	))
	return NewSimpleSessionColumn(
		columnID,
		field,
		func(session *schema.Session) (any, error) {
			uniqueEvents := make(map[string]bool)
			for _, event := range session.Events {
				valueAsString, ok := event.Values[CoreInterfaces.EventName.Field.Name].(string)
				if !ok {
					continue
				}
				if slices.Contains(eventNames, valueAsString) {
					depHash := ""
					for _, dependentColumn := range dependentColumns {
						depValue, ok := event.Values[dependentColumn.Name]
						if !ok {
							continue
						}
						depHash += fmt.Sprintf("%v||%v", valueAsString, depValue)
					}
					uniqueEvents[depHash] = true
				}
			}
			return len(uniqueEvents), nil
		},
		options...,
	)
}
