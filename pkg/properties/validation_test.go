package properties

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSettings(t *testing.T) {
	testCases := []struct {
		name     string
		settings *Settings
		wantErr  string
	}{
		{
			name:     "valid level 0",
			settings: &Settings{IPMaskingLevel: 0},
		},
		{
			name:     "valid level 1",
			settings: &Settings{IPMaskingLevel: 1},
		},
		{
			name:     "valid level 2",
			settings: &Settings{IPMaskingLevel: 2},
		},
		{
			name:     "valid level 3",
			settings: &Settings{IPMaskingLevel: 3},
		},
		{
			name:     "valid level 4",
			settings: &Settings{IPMaskingLevel: 4},
		},
		{
			name:     "invalid negative level",
			settings: &Settings{IPMaskingLevel: -1},
			wantErr:  "ip masking level must be between 0 and 4: -1",
		},
		{
			name:     "invalid large level",
			settings: &Settings{IPMaskingLevel: 5},
			wantErr:  "ip masking level must be between 0 and 4: 5",
		},
		{
			name: "partial masking is valid with session stamp join",
			settings: &Settings{
				IPMaskingLevel:            1,
				SessionJoinBySessionStamp: true,
			},
		},
		{
			name: "full masking conflicts with session stamp join",
			settings: &Settings{
				IPMaskingLevel:            4,
				SessionJoinBySessionStamp: true,
			},
			wantErr: "session join by session stamp must be disabled when ip masking level is 4",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// given

			// when
			err := ValidateSettings(testCase.settings)

			// then
			if testCase.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			assert.Equal(t, testCase.wantErr, err.Error())
		})
	}
}
