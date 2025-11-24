package properties

import (
	"fmt"
)

// SettingsRegistry is a registry of property configurations.
type SettingsRegistry interface {
	GetByMeasurementID(trackingID string) (Settings, error)
	GetByPropertyID(propertyID string) (Settings, error)
}

// StaticSettingsRegistry is a static property settings
// registry that stores property settings in a map.
type StaticSettingsRegistry struct {
	tid           map[string]*Settings
	pid           map[string]*Settings
	defaultConfig *Settings
}

// GetByMeasurementID gets a property configuration by tracking ID.
func (s StaticSettingsRegistry) GetByMeasurementID(trackingID string) (Settings, error) {
	property, ok := s.tid[trackingID]
	if !ok {
		if s.defaultConfig != nil {
			return *s.defaultConfig, nil
		}
		return Settings{}, fmt.Errorf("unknown property tracking ID: %s", trackingID)
	}
	return *property, nil
}

// GetByPropertyID gets a property configuration by property ID.
func (s StaticSettingsRegistry) GetByPropertyID(propertyID string) (Settings, error) {
	property, ok := s.pid[propertyID]
	if !ok {
		if s.defaultConfig != nil {
			return *s.defaultConfig, nil
		}
		return Settings{}, fmt.Errorf("unknown property ID: %s", propertyID)
	}
	return *property, nil
}

// StaticSettingsRegistryOptions are options for the static property source.
type StaticSettingsRegistryOptions func(s *StaticSettingsRegistry)

// WithDefaultConfig sets the default configuration for the static property source.
func WithDefaultConfig(config Settings) StaticSettingsRegistryOptions {
	return func(s *StaticSettingsRegistry) {
		s.defaultConfig = &config
	}
}

// NewStaticSettingsRegistry creates a new static property source from a list of property configurations.
func NewStaticSettingsRegistry(props []Settings, opts ...StaticSettingsRegistryOptions) SettingsRegistry {
	tid := make(map[string]*Settings)
	pid := make(map[string]*Settings)
	for _, prop := range props {
		tid[prop.PropertyMeasurementID] = &prop
		pid[prop.PropertyID] = &prop
	}
	s := &StaticSettingsRegistry{tid: tid, pid: pid}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
