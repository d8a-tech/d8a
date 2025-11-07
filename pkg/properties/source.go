package properties

import (
	"fmt"
)

// PropertySource is a source of property configurations.
type PropertySource interface {
	GetByMeasurementID(trackingID string) (PropertyConfig, error)
	GetByPropertyID(propertyID string) (PropertyConfig, error)
}

// StaticPropertySource is a static property source that stores property configurations in a map.
type StaticPropertySource struct {
	tid           map[string]*PropertyConfig
	pid           map[string]*PropertyConfig
	defaultConfig *PropertyConfig
}

// GetByMeasurementID gets a property configuration by tracking ID.
func (s StaticPropertySource) GetByMeasurementID(trackingID string) (PropertyConfig, error) {
	property, ok := s.tid[trackingID]
	if !ok {
		if s.defaultConfig != nil {
			return *s.defaultConfig, nil
		}
		return PropertyConfig{}, fmt.Errorf("Unknown property tracking ID: %s", trackingID)
	}
	return *property, nil
}

// GetByPropertyID gets a property configuration by property ID.
func (s StaticPropertySource) GetByPropertyID(propertyID string) (PropertyConfig, error) {
	property, ok := s.pid[propertyID]
	if !ok {
		if s.defaultConfig != nil {
			return *s.defaultConfig, nil
		}
		return PropertyConfig{}, fmt.Errorf("Unknown property ID: %s", propertyID)
	}
	return *property, nil
}

// StaticPropertySourceOptions are options for the static property source.
type StaticPropertySourceOptions func(s *StaticPropertySource)

// WithDefaultConfig sets the default configuration for the static property source.
func WithDefaultConfig(config PropertyConfig) StaticPropertySourceOptions {
	return func(s *StaticPropertySource) {
		s.defaultConfig = &config
	}
}

// NewStaticPropertySource creates a new static property source from a list of property configurations.
func NewStaticPropertySource(props []PropertyConfig, opts ...StaticPropertySourceOptions) PropertySource {
	tid := make(map[string]*PropertyConfig)
	pid := make(map[string]*PropertyConfig)
	for _, prop := range props {
		tid[prop.PropertyMeasurementID] = &prop
		pid[prop.PropertyID] = &prop
	}
	s := &StaticPropertySource{tid: tid, pid: pid}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
