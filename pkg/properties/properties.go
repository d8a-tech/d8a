// Package properties provides the core data models and types for property configuration.
package properties

import "time"

// PropertySettings holds the tracking configuration for a property.
type PropertySettings struct {
	PropertyID   string
	PropertyName string
	// In some cases (like matomo) the measurement ID is the same as the property ID.
	// In other cases (like GA4) the measurement ID is a separate ID.
	PropertyMeasurementID string

	SplitByUserID              bool
	SplitByCampaign            bool
	SplitByTimeSinceFirstEvent time.Duration
	SplitByMaxEvents           int
}
