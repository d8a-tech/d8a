package customcolumns

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// PropertyCustomColumnsRegistry resolves and builds custom columns per property.
type PropertyCustomColumnsRegistry struct {
	SettingsRegistry properties.SettingsRegistry
	Builder          Registry
}

// NewPropertyCustomColumnsRegistry creates a per-property custom columns registry.
func NewPropertyCustomColumnsRegistry(psr properties.SettingsRegistry, builder Registry) schema.ColumnsRegistry {
	if builder == nil {
		builder = NewRegistry(nil)
	}

	return &PropertyCustomColumnsRegistry{
		SettingsRegistry: psr,
		Builder:          builder,
	}
}

// Get returns only generated custom columns for the requested property.
func (r *PropertyCustomColumnsRegistry) Get(propertyID string) (schema.Columns, error) {
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
