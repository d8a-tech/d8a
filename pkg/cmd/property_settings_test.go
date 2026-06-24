package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func newPropertySettingsFlagsForConfigTests() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        configFlag.Name,
			Aliases:     append([]string(nil), configFlag.Aliases...),
			Value:       configFlag.Value,
			Usage:       configFlag.Usage,
			Destination: &configFile,
		},
		&cli.StringFlag{Name: protocolFlag.Name, Value: ""},
		&cli.StringFlag{Name: propertyIDFlag.Name, Value: ""},
		&cli.StringFlag{Name: propertyNameFlag.Name, Value: ""},
		&cli.BoolFlag{
			Name:    propertySettingsSplitByUserIDFlag.Name,
			Sources: defaultSourceChain("PROPERTY_SETTINGS_SPLIT_BY_USER_ID", "property.settings.split_by_user_id"),
			Value:   true,
		},
		&cli.BoolFlag{
			Name:    propertySettingsSplitByCampaignFlag.Name,
			Sources: defaultSourceChain("PROPERTY_SETTINGS_SPLIT_BY_CAMPAIGN", "property.settings.split_by_campaign"),
			Value:   true,
		},
		&cli.DurationFlag{
			Name: propertySettingsSplitByTimeSinceFirstEventFlag.Name,
			Sources: defaultSourceChain(
				"PROPERTY_SETTINGS_SPLIT_BY_TIME_SINCE_FIRST_EVENT",
				"property.settings.split_by_time_since_first_event",
			),
			Value: 12 * time.Hour,
		},
		&cli.IntFlag{
			Name:    propertySettingsSplitByMaxEventsFlag.Name,
			Sources: defaultSourceChain("PROPERTY_SETTINGS_SPLIT_BY_MAX_EVENTS", "property.settings.split_by_max_events"),
			Value:   1000,
		},
		&cli.StringSliceFlag{
			Name:    propertySettingsExcludedURLParamsFlag.Name,
			Sources: defaultSourceChain("PROPERTY_SETTINGS_EXCLUDED_URL_PARAMS", "property.settings.excluded_url_params"),
			Value:   historicalExcludedURLParams,
		},
		&cli.DurationFlag{
			Name:    sessionsTimeoutFlag.Name,
			Sources: defaultSourceChain("SESSIONS_TIMEOUT", "sessions.timeout"),
			Value:   30 * time.Minute,
		},
		&cli.BoolFlag{
			Name:    sessionsJoinBySessionStampFlag.Name,
			Sources: defaultSourceChain("SESSIONS_JOIN_BY_SESSION_STAMP", "sessions.join_by_session_stamp"),
			Value:   true,
		},
		&cli.BoolFlag{
			Name:    sessionsJoinByUserIDFlag.Name,
			Sources: defaultSourceChain("SESSIONS_JOIN_BY_USER_ID", "sessions.join_by_user_id"),
			Value:   true,
		},
		&cli.IntFlag{
			Name:    propertySettingsIPMaskingLevelFlag.Name,
			Sources: defaultSourceChain("PROPERTY_SETTINGS_IP_MASKING_LEVEL", "property.settings.ip_masking_level"),
			Value:   0,
		},
		&cli.StringSliceFlag{
			Name:    filtersFieldsFlag.Name,
			Sources: defaultSourceChain("FILTERS_FIELDS", "filters.fields"),
		},
		&cli.StringSliceFlag{
			Name:    filtersConditionsFlag.Name,
			Sources: defaultSourceChain("FILTERS_CONDITIONS", "filters.conditions"),
		},
		&cli.StringFlag{
			Name:    ga4ParamsFlag.Name,
			Sources: ga4ParamsFlag.Sources,
		},
		&cli.StringFlag{
			Name:    matomoCustomDimensionsFlag.Name,
			Sources: matomoCustomDimensionsFlag.Sources,
		},
		&cli.StringFlag{
			Name:    matomoCustomVariablesFlag.Name,
			Sources: matomoCustomVariablesFlag.Sources,
		},
	}
}

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
			assert.Equal(t, 0, settings.IPMaskingLevel)
			return nil
		},
	}

	require.NoError(t, app.Run(context.Background(), args))
}

func TestPropertySettings_OverrideIPMaskingLevelFromConfig(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")
	configPath := writeConfigFile(t, `
property:
  settings:
    ip_masking_level: 2
sessions:
  join_by_session_stamp: false
`)
	setConfigFileForTest(t, configPath)
	args := []string{"d8a-test", "--config=" + configPath}
	setCurrentRunArgsForTest(t, args)

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: mergeFlags([]cli.Flag{configFlag}, getServerFlags()),
		Action: func(_ context.Context, cmd *cli.Command) error {
			// when
			settings, err := propertySettings(cmd).GetByPropertyID(cmd.String(propertyIDFlag.Name))
			require.NoError(t, err)

			// then
			assert.Equal(t, 2, settings.IPMaskingLevel)
			return nil
		},
	}

	require.NoError(t, app.Run(context.Background(), args))
}

func TestPropertySettings_OverrideIPMaskingLevelFromCLI(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")
	args := []string{
		"d8a-test",
		"--property-settings-ip-masking-level=3",
		"--sessions-join-by-session-stamp=false",
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
			assert.Equal(t, 3, settings.IPMaskingLevel)
			return nil
		},
	}

	require.NoError(t, app.Run(context.Background(), args))
}

func TestPropertySettings_InvalidIPMaskingLevelFromConfigPanics(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")
	configPath := writeConfigFile(t, `
property:
  settings:
    ip_masking_level: 5
sessions:
  join_by_session_stamp: false
`)
	setConfigFileForTest(t, configPath)
	args := []string{"d8a-test", "--config=" + configPath}
	setCurrentRunArgsForTest(t, args)

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: newPropertySettingsFlagsForConfigTests(),
		Action: func(_ context.Context, cmd *cli.Command) error {
			_, _ = propertySettings(cmd).GetByPropertyID(cmd.String(propertyIDFlag.Name))
			return nil
		},
	}

	// then
	assert.Panics(t, func() {
		require.NoError(t, app.Run(context.Background(), args))
	})
}

func TestPropertySettings_IPMaskingSessionStampConflictFromConfigPanics(t *testing.T) {
	// given
	setDeliveryModeForTest(t, "")
	configPath := writeConfigFile(t, `
property:
  settings:
    ip_masking_level: 1
sessions:
  join_by_session_stamp: true
`)
	setConfigFileForTest(t, configPath)
	args := []string{"d8a-test", "--config=" + configPath}
	setCurrentRunArgsForTest(t, args)

	app := &cli.Command{
		Name:  "d8a-test",
		Flags: newPropertySettingsFlagsForConfigTests(),
		Action: func(_ context.Context, cmd *cli.Command) error {
			_, _ = propertySettings(cmd).GetByPropertyID(cmd.String(propertyIDFlag.Name))
			return nil
		},
	}

	// then
	assert.Panics(t, func() {
		require.NoError(t, app.Run(context.Background(), args))
	})
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
