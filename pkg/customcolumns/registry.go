package customcolumns

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// Builder builds runtime columns from normalized custom column configs.
type Builder interface {
	BuildAll(defs []properties.CustomColumnConfig) (schema.Columns, error)
}

type builder struct {
	factory Factory
}

// BuilderOption configures the custom-column builder.
type BuilderOption func(*builder)

// WithFactory overrides the factory used to build custom columns.
func WithFactory(factory Factory) BuilderOption {
	return func(r *builder) {
		if factory == nil {
			return
		}
		r.factory = factory
	}
}

// NewBuilder creates a runtime custom-column builder.
func NewBuilder(opts ...BuilderOption) Builder {
	r := &builder{factory: NewFactory()}
	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *builder) BuildAll(defs []properties.CustomColumnConfig) (schema.Columns, error) {
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

// NewRegistry is kept for backward compatibility.
func NewRegistry(opts ...BuilderOption) Builder {
	return NewBuilder(opts...)
}
