package customcolumns

import (
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// Builder builds runtime columns from normalized custom column configs.
type Builder interface {
	BuildAll(defs []properties.CustomColumnConfig) (schema.Columns, error)
}

type builder struct {
	columnBuilder ColumnBuilder
}

// NewBuilder creates a runtime custom-column builder.
func NewBuilder() Builder {
	return &builder{
		columnBuilder: NewMultiColumnBuilder(
			NewEventColumnBuilder(),
			NewSessionColumnBuilder(),
		),
	}
}

func (r *builder) BuildAll(defs []properties.CustomColumnConfig) (schema.Columns, error) {
	defPtrs := make([]*properties.CustomColumnConfig, 0, len(defs))
	for i := range defs {
		defPtrs = append(defPtrs, &defs[i])
	}

	builtColumns, err := r.columnBuilder.Build(defPtrs)
	if err != nil {
		return schema.Columns{}, err
	}

	built := schema.Columns{}
	for i := range builtColumns {
		column := builtColumns[i]
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
func NewRegistry() Builder {
	return NewBuilder()
}
