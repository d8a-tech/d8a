package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestPropertySettings_DefaultExcludedURLParams(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")
	args := []string{"d8a-test"}
	setCurrentRunArgsForTest(t, args)

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(_ context.Context, cmd *cli.Command) error {
			// when
			settings, err := propertySettings(cmd).GetByPropertyID(cmd.String(propertyIDFlag.Name))
			require.NoError(t, err)

			// then
			assert.Equal(t, historicalExcludedURLParams, settings.ExcludedURLParamsSafe())
			return nil
		},
	}

	require.NoError(t, app.Run(context.Background(), args))
}

func TestPropertySettings_OverrideExcludedURLParams(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")
	args := []string{
		"d8a-test",
		"--property-settings-excluded-url-params=utm_source",
		"--property-settings-excluded-url-params=custom_param",
	}
	setCurrentRunArgsForTest(t, args)

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(_ context.Context, cmd *cli.Command) error {
			// when
			settings, err := propertySettings(cmd).GetByPropertyID(cmd.String(propertyIDFlag.Name))
			require.NoError(t, err)

			// then
			assert.Equal(t, []string{"utm_source", "custom_param"}, settings.ExcludedURLParamsSafe())
			return nil
		},
	}

	require.NoError(t, app.Run(context.Background(), args))
}
