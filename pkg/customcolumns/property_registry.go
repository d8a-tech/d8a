package customcolumns

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// CustomColumnsPropertySettingsRegistry resolves and builds custom columns per property.
type CustomColumnsPropertySettingsRegistry struct {
	SettingsRegistry properties.SettingsRegistry
	Builder          Builder
}

// NewCustomColumnsPropertySettingsRegistry creates a per-property custom columns registry.
func NewCustomColumnsPropertySettingsRegistry(psr properties.SettingsRegistry, builder Builder) schema.ColumnsRegistry {
	if builder == nil {
		builder = NewBuilder()
	}

	return &CustomColumnsPropertySettingsRegistry{
		SettingsRegistry: psr,
		Builder:          builder,
	}
}

// NewPropertyCustomColumnsRegistry is kept for backward compatibility.
func NewPropertyCustomColumnsRegistry(psr properties.SettingsRegistry, builder Builder) schema.ColumnsRegistry {
	return NewCustomColumnsPropertySettingsRegistry(psr, builder)
}

// Get returns only generated custom columns for the requested property.
func (r *CustomColumnsPropertySettingsRegistry) Get(propertyID string) (schema.Columns, error) {
	settings, err := r.SettingsRegistry.GetByPropertyID(propertyID)
	if err != nil {
		return schema.Columns{}, fmt.Errorf("get settings for property %q: %w", propertyID, err)
	}

	columns, err := r.Builder.BuildAll(settings.CustomColumnsSafe())
	if err != nil {
		return schema.Columns{}, fmt.Errorf("build custom columns for property %q: %w", propertyID, err)
	}

	return columns, nil
}
