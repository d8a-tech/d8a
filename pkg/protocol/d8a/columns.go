package d8a

import (
	"strings"

	"github.com/d8a-tech/d8a/pkg/schema"
)

func patchInterfaceID(id schema.InterfaceID) schema.InterfaceID {
	return schema.InterfaceID(strings.Replace(string(id), "ga4.", "d8a.", 1))
}

type d8aSessionColumnWrapper struct {
	column schema.SessionColumn
}

func (c *d8aSessionColumnWrapper) Docs() schema.Documentation {
	return c.column.Docs()
}

func (c *d8aSessionColumnWrapper) Implements() schema.Interface {
	childInterface := c.column.Implements()
	return schema.Interface{
		ID:    patchInterfaceID(childInterface.ID),
		Field: childInterface.Field,
	}
}

func (c *d8aSessionColumnWrapper) DependsOn() []schema.DependsOnEntry {
	childDependsOn := c.column.DependsOn()
	deps := make([]schema.DependsOnEntry, len(childDependsOn))
	copy(deps, childDependsOn)
	for i := range deps {
		deps[i].Interface = patchInterfaceID(deps[i].Interface)
	}
	return deps
}

func (c *d8aSessionColumnWrapper) Write(session *schema.Session) schema.D8AColumnWriteError {
	return c.column.Write(session)
}

type d8aEventColumnWrapper struct {
	column schema.EventColumn
}

func (c *d8aEventColumnWrapper) Docs() schema.Documentation {
	return c.column.Docs()
}

func (c *d8aEventColumnWrapper) Implements() schema.Interface {
	childInterface := c.column.Implements()
	return schema.Interface{
		ID:    patchInterfaceID(childInterface.ID),
		Field: childInterface.Field,
	}
}

func (c *d8aEventColumnWrapper) DependsOn() []schema.DependsOnEntry {
	childDependsOn := c.column.DependsOn()
	deps := make([]schema.DependsOnEntry, len(childDependsOn))
	copy(deps, childDependsOn)
	for i := range deps {
		deps[i].Interface = patchInterfaceID(deps[i].Interface)
	}
	return deps
}

func (c *d8aEventColumnWrapper) Write(event *schema.Event) schema.D8AColumnWriteError {
	return c.column.Write(event)
}

type d8aSessionScopedEventColumnWrapper struct {
	column schema.SessionScopedEventColumn
}

func (c *d8aSessionScopedEventColumnWrapper) Docs() schema.Documentation {
	return c.column.Docs()
}

func (c *d8aSessionScopedEventColumnWrapper) Implements() schema.Interface {
	childInterface := c.column.Implements()
	return schema.Interface{
		ID:    patchInterfaceID(childInterface.ID),
		Field: childInterface.Field,
	}
}

func (c *d8aSessionScopedEventColumnWrapper) DependsOn() []schema.DependsOnEntry {
	childDependsOn := c.column.DependsOn()
	deps := make([]schema.DependsOnEntry, len(childDependsOn))
	copy(deps, childDependsOn)
	for i := range deps {
		deps[i].Interface = patchInterfaceID(deps[i].Interface)
	}
	return deps
}

func (c *d8aSessionScopedEventColumnWrapper) Write(session *schema.Session, i int) schema.D8AColumnWriteError {
	return c.column.Write(session, i)
}

func WrapColumns(columns schema.Columns, options ...OptionFunc) schema.Columns {
	opts := defaultOptions()
	for _, option := range options {
		option(&opts)
	}
	allSkipped := map[schema.InterfaceID]bool{}
	for _, skip := range opts.SkipColumns {
		allSkipped[skip] = true
	}
	eventPatches := map[schema.InterfaceID]schema.EventColumn{}
	for _, patch := range opts.PatchEvents {
		eventPatches[patch.InterfaceID] = patch.column
	}
	sessionPatches := map[schema.InterfaceID]schema.SessionColumn{}
	for _, patch := range opts.PatchSessions {
		sessionPatches[patch.InterfaceID] = patch.column
	}
	sessionScopedEventPatches := map[schema.InterfaceID]schema.SessionScopedEventColumn{}
	for _, patch := range opts.PatchSessionScopedEvents {
		sessionScopedEventPatches[patch.InterfaceID] = patch.column
	}
	newColumns := schema.Columns{
		Session:            make([]schema.SessionColumn, 0, len(columns.Session)),
		Event:              make([]schema.EventColumn, 0, len(columns.Event)),
		SessionScopedEvent: make([]schema.SessionScopedEventColumn, 0, len(columns.SessionScopedEvent)),
	}
	for _, column := range columns.Session {
		if allSkipped[column.Implements().ID] {
			continue
		}
		if patch, ok := sessionPatches[column.Implements().ID]; ok {
			newColumns.Session = append(newColumns.Session, &d8aSessionColumnWrapper{column: patch})
		} else {
			newColumns.Session = append(newColumns.Session, &d8aSessionColumnWrapper{column: column})
		}
	}
	for _, column := range columns.Event {
		if allSkipped[column.Implements().ID] {
			continue
		}
		if patch, ok := eventPatches[column.Implements().ID]; ok {
			newColumns.Event = append(newColumns.Event, &d8aEventColumnWrapper{column: patch})
		} else {
			newColumns.Event = append(newColumns.Event, &d8aEventColumnWrapper{column: column})
		}
	}
	for _, column := range columns.SessionScopedEvent {
		if allSkipped[column.Implements().ID] {
			continue
		}
		if patch, ok := sessionScopedEventPatches[column.Implements().ID]; ok {
			newColumns.SessionScopedEvent = append(
				newColumns.SessionScopedEvent,
				&d8aSessionScopedEventColumnWrapper{column: patch},
			)
		} else {
			newColumns.SessionScopedEvent = append(
				newColumns.SessionScopedEvent,
				&d8aSessionScopedEventColumnWrapper{column: column},
			)
		}
	}
	return newColumns
}

type PatchEvent struct {
	InterfaceID schema.InterfaceID
	column      schema.EventColumn
}

type PatchSession struct {
	InterfaceID schema.InterfaceID
	column      schema.SessionColumn
}

type PatchSessionScopedEvent struct {
	InterfaceID schema.InterfaceID
	column      schema.SessionScopedEventColumn
}

type WrapColumnOptionsS struct {
	SkipColumns              []schema.InterfaceID
	PatchEvents              []PatchEvent
	PatchSessions            []PatchSession
	PatchSessionScopedEvents []PatchSessionScopedEvent
}

type OptionFunc func(opts *WrapColumnOptionsS)

func WithSkipColumns(columns ...schema.InterfaceID) OptionFunc {
	return func(opts *WrapColumnOptionsS) {
		opts.SkipColumns = append(opts.SkipColumns, columns...)
	}
}

func WithPatchEvent(interfaceID schema.InterfaceID, column schema.EventColumn) OptionFunc {
	return func(opts *WrapColumnOptionsS) {
		opts.PatchEvents = append(opts.PatchEvents, PatchEvent{InterfaceID: interfaceID, column: column})
	}
}

func WithPatchSession(interfaceID schema.InterfaceID, column schema.SessionColumn) OptionFunc {
	return func(opts *WrapColumnOptionsS) {
		opts.PatchSessions = append(opts.PatchSessions, PatchSession{InterfaceID: interfaceID, column: column})
	}
}

func WithPatchSessionScopedEvent(interfaceID schema.InterfaceID, column schema.SessionScopedEventColumn) OptionFunc {
	return func(opts *WrapColumnOptionsS) {
		opts.PatchSessionScopedEvents = append(
			opts.PatchSessionScopedEvents,
			PatchSessionScopedEvent{InterfaceID: interfaceID, column: column},
		)
	}
}

func defaultOptions() WrapColumnOptionsS {
	return WrapColumnOptionsS{
		SkipColumns:              []schema.InterfaceID{},
		PatchEvents:              []PatchEvent{},
		PatchSessions:            []PatchSession{},
		PatchSessionScopedEvents: []PatchSessionScopedEvent{},
	}
}
