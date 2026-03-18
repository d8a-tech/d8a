package customcolumns

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// CustomColumnsPropertySettingsRegistry resolves and builds custom columns per property.
type CustomColumnsPropertySettingsRegistry struct {
	SettingsRegistry properties.SettingsRegistry
	Builder          ColumnBuilder
}

// NewCustomColumnsPropertySettingsRegistry creates a per-property custom columns registry.
func NewCustomColumnsPropertySettingsRegistry(
	psr properties.SettingsRegistry,
	builder ColumnBuilder,
) schema.ColumnsRegistry {
	if builder == nil {
		builder = NewBuilder()
	}

	return &CustomColumnsPropertySettingsRegistry{
		SettingsRegistry: psr,
		Builder:          builder,
	}
}

// NewPropertyCustomColumnsRegistry is kept for backward compatibility.
func NewPropertyCustomColumnsRegistry(psr properties.SettingsRegistry, builder ColumnBuilder) schema.ColumnsRegistry {
	return NewCustomColumnsPropertySettingsRegistry(psr, builder)
}

// Get returns only generated custom columns for the requested property.
func (r *CustomColumnsPropertySettingsRegistry) Get(propertyID string) (schema.Columns, error) {
	settings, err := r.SettingsRegistry.GetByPropertyID(propertyID)
	if err != nil {
		return schema.Columns{}, fmt.Errorf("get settings for property %q: %w", propertyID, err)
	}

	defs := settings.CustomColumnsSafe()
	defPtrs := make([]*properties.CustomColumnConfig, 0, len(defs))
	for i := range defs {
		defPtrs = append(defPtrs, &defs[i])
	}

	builtColumns, err := r.Builder.Build(defPtrs)
	if err != nil {
		return schema.Columns{}, fmt.Errorf("build custom columns for property %q: %w", propertyID, err)
	}

	columns := schema.Columns{}
	for i := range builtColumns {
		column := builtColumns[i]
		if column.Event != nil {
			columns.Event = append(columns.Event, column.Event)
		}
		if column.Session != nil {
			columns.Session = append(columns.Session, column.Session)
		}
		if column.SessionScopedEvent != nil {
			columns.SessionScopedEvent = append(columns.SessionScopedEvent, column.SessionScopedEvent)
		}
	}

	return columns, nil
}
