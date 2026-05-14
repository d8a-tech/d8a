package properties

import "time"

// TestSettingsOption configures test settings.
type TestSettingsOption func(*Settings)

// WithSessionTimeout sets the session timeout for test settings.
func WithSessionTimeout(d time.Duration) TestSettingsOption {
	return func(s *Settings) {
		s.SessionTimeout = d
	}
}

// WithExcludedURLParams sets excluded URL params for test settings.
func WithExcludedURLParams(params []string) TestSettingsOption {
	return func(s *Settings) {
		s.ExcludedURLParams = params
	}
}

// NewTestSettingRegistry is a test property source that returns a static property configuration.
func NewTestSettingRegistry(opts ...TestSettingsOption) SettingsRegistry {
	settings := &Settings{
		PropertyID:            "1234567890",
		PropertyName:          "Test Property",
		PropertyMeasurementID: "G-2VEWJC5YPE",
		SessionTimeout:        30 * time.Second,

		SessionJoinBySessionStamp: true,
		SessionJoinByUserID:       true,
	}
	for _, opt := range opts {
		opt(settings)
	}
	return NewStaticSettingsRegistry([]Settings{}, WithDefaultConfig(settings))
}
