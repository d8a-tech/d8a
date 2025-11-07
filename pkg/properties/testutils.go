package properties

// TestPropertySource is a test property source that returns a static property configuration.
func TestPropertySource() PropertySource {
	return NewStaticPropertySource([]PropertyConfig{}, WithDefaultConfig(PropertyConfig{
		PropertyID:            "1234567890",
		PropertyName:          "Test Property",
		PropertyMeasurementID: "G-2VEWJC5YPE",
	}))
}
