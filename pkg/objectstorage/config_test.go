package objectstorage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		config    *config
		expectErr bool
		errField  string
	}{
		{
			name:      "valid config",
			config:    defaultConfig(),
			expectErr: false,
		},
		{
			name: "invalid MaxItemsToReadAtOnce - zero",
			config: &config{
				MaxItemsToReadAtOnce: 0,
				MinInterval:          time.Second,
				IntervalExpFactor:    1.1,
				MaxInterval:          time.Minute,
				ProcessingTimeout:    time.Minute,
				RetryAttempts:        1,
			},
			expectErr: true,
			errField:  "MaxItemsToReadAtOnce",
		},
		{
			name: "invalid MaxItemsToReadAtOnce - negative",
			config: &config{
				MaxItemsToReadAtOnce: -1,
				MinInterval:          time.Second,
				IntervalExpFactor:    1.1,
				MaxInterval:          time.Minute,
				ProcessingTimeout:    time.Minute,
				RetryAttempts:        1,
			},
			expectErr: true,
			errField:  "MaxItemsToReadAtOnce",
		},
		{
			name: "invalid ProcessingTimeout - zero",
			config: &config{
				MaxItemsToReadAtOnce: 50,
				MinInterval:          time.Second,
				IntervalExpFactor:    1.1,
				MaxInterval:          time.Minute,
				ProcessingTimeout:    0,
				RetryAttempts:        1,
			},
			expectErr: true,
			errField:  "ProcessingTimeout",
		},
		{
			name: "invalid RetryAttempts - negative",
			config: &config{
				MaxItemsToReadAtOnce: 50,
				MinInterval:          time.Second,
				IntervalExpFactor:    1.1,
				MaxInterval:          time.Minute,
				ProcessingTimeout:    time.Minute,
				RetryAttempts:        -1,
			},
			expectErr: true,
			errField:  "RetryAttempts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			err := tt.config.Validate()

			// then
			if tt.expectErr {
				assert.Error(t, err)
				var configErr errInvalidConfig
				assert.ErrorAs(t, err, &configErr)
				assert.Equal(t, tt.errField, configErr.Field)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestErrInvalidConfig(t *testing.T) {
	// given
	err := errInvalidConfig{
		Field:   "TestField",
		Message: "test message",
	}

	// when
	result := err.Error()

	// then
	assert.Equal(t, "invalid config field 'TestField': test message", result)
}
