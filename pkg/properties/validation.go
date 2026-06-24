package properties

import "fmt"

// ValidateSettings validates property settings.
func ValidateSettings(settings *Settings) error {
	if settings == nil {
		return fmt.Errorf("settings must not be nil")
	}

	if settings.IPMaskingLevel < 0 || settings.IPMaskingLevel > 4 {
		return fmt.Errorf("ip masking level must be between 0 and 4: %d", settings.IPMaskingLevel)
	}

	if settings.IPMaskingLevel > 0 && settings.SessionJoinBySessionStamp {
		return fmt.Errorf("session join by session stamp must be disabled when ip masking level is non-zero")
	}

	return nil
}
