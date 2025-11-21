package properties

// NewTestSettingRegistry is a test property source that returns a static property configuration.
func NewTestSettingRegistry() SettingsRegistry {
	return NewStaticSettingsRegistry([]Settings{}, WithDefaultConfig(Settings{
		PropertyID:            "1234567890",
		PropertyName:          "Test Property",
		PropertyMeasurementID: "G-2VEWJC5YPE",

		SessionJoinBySessionStamp: true,
		SessionJoinByUserID:       true,
	}))
}
