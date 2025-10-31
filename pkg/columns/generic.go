// Package columns provides generic column implementations for session data tracking.
package columns

import (
	"errors"
	"fmt"
	"net/url"
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
func CastToBool(columnID schema.InterfaceID) func(any) (any, error) {
	return func(value any) (any, error) {
		// Handle boolean values directly
		if boolVal, ok := value.(bool); ok {
			return boolVal, nil
		}

		// Handle string values
		valueStr, ok := value.(string)
		if !ok {
			logrus.Debugf("CastToBool: %s: value is not a string or bool: %v", columnID, value)
			return false, nil
		}

		// Use util.StrToBool for string conversion
		boolVal, err := util.StrToBool(valueStr)
		if err != nil {
			logrus.Debugf("CastToBool: %s: %v: %v", columnID, err, value)
			return false, err
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
	write     func(*simpleSessionScopedEventColumn, *schema.Event, *schema.Session) error
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

func (c *simpleSessionScopedEventColumn) Write(event *schema.Event, session *schema.Session) error {
	return c.write(c, event, session)
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
	getValue func(*schema.Event, *schema.Session) (any, error),
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
	c.write = func(c *simpleSessionScopedEventColumn, event *schema.Event, session *schema.Session) error {
		value, err := getValue(event, session)
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
