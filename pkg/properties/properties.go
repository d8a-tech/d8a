// Package properties provides the core data models and types for property configuration.
package properties

// PropertyConfig holds the tracking configuration for a property.
type PropertyConfig struct {
	PropertyID   string
	PropertyName string
	// In some cases (like matomo) the measurement ID is the same as the property ID.
	// In other cases (like GA4) the measurement ID is a separate ID.
	PropertyMeasurementID string
	Settings              map[string]any
}
