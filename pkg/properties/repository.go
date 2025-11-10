package properties

import (
	"fmt"
)

// SettingsRegistry is a registry of property configurations.
type SettingsRegistry interface {
	GetByMeasurementID(trackingID string) (PropertySettings, error)
	GetByPropertyID(propertyID string) (PropertySettings, error)
}

// StaticPropertySettingsRegistry is a static property settings
// registry that stores property settings in a map.
type StaticPropertySettingsRegistry struct {
	tid           map[string]*PropertySettings
	pid           map[string]*PropertySettings
	defaultConfig *PropertySettings
}

// GetByMeasurementID gets a property configuration by tracking ID.
func (s StaticPropertySettingsRegistry) GetByMeasurementID(trackingID string) (PropertySettings, error) {
	property, ok := s.tid[trackingID]
	if !ok {
		if s.defaultConfig != nil {
			return *s.defaultConfig, nil
		}
		return PropertySettings{}, fmt.Errorf("Unknown property tracking ID: %s", trackingID)
	}
	return *property, nil
}

// GetByPropertyID gets a property configuration by property ID.
func (s StaticPropertySettingsRegistry) GetByPropertyID(propertyID string) (PropertySettings, error) {
	property, ok := s.pid[propertyID]
	if !ok {
		if s.defaultConfig != nil {
			return *s.defaultConfig, nil
		}
		return PropertySettings{}, fmt.Errorf("Unknown property ID: %s", propertyID)
	}
	return *property, nil
}

// StaticPropertySourceOptions are options for the static property source.
type StaticPropertySourceOptions func(s *StaticPropertySettingsRegistry)

// WithDefaultConfig sets the default configuration for the static property source.
func WithDefaultConfig(config PropertySettings) StaticPropertySourceOptions {
	return func(s *StaticPropertySettingsRegistry) {
		s.defaultConfig = &config
	}
}

// NewStaticPropertySource creates a new static property source from a list of property configurations.
func NewStaticPropertySource(props []PropertySettings, opts ...StaticPropertySourceOptions) SettingsRegistry {
	tid := make(map[string]*PropertySettings)
	pid := make(map[string]*PropertySettings)
	for _, prop := range props {
		tid[prop.PropertyMeasurementID] = &prop
		pid[prop.PropertyID] = &prop
	}
	s := &StaticPropertySettingsRegistry{tid: tid, pid: pid}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
