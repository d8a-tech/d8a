package customcolumns

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// Registry builds runtime columns from normalized custom column configs.
type Registry interface {
	BuildAll(defs []properties.CustomColumnConfig) (schema.Columns, error)
}

type registry struct {
	factory Factory
}

// RegistryOption configures the custom-column registry.
type RegistryOption func(*registry)

// WithFactory overrides the factory used to build custom columns.
func WithFactory(factory Factory) RegistryOption {
	return func(r *registry) {
		if factory == nil {
			return
		}
		r.factory = factory
	}
}

// NewRegistry creates a runtime custom-column registry.

func NewRegistry(opts ...RegistryOption) Registry {
	r := &registry{factory: NewFactory()}
	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *registry) BuildAll(defs []properties.CustomColumnConfig) (schema.Columns, error) {
	built := schema.Columns{}

	for i := range defs {
		def := &defs[i]
		column, err := r.factory.Build(def)
		if err != nil {
			return schema.Columns{}, fmt.Errorf("build custom column %q at index %d: %w", def.Name, i, err)
		}

		if column.Event != nil {
			built.Event = append(built.Event, column.Event)
		}
		if column.Session != nil {
			built.Session = append(built.Session, column.Session)
		}
		if column.SessionScopedEvent != nil {
			built.SessionScopedEvent = append(built.SessionScopedEvent, column.SessionScopedEvent)
		}
	}

	return built, nil
}
