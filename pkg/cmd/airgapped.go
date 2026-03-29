package cmd

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

var airgapped bool

var currentRunArgs []string

var airgappedOverrideRules []*modeOverrideRule

func customAirgappedSourceChain(
	flagName, configKey, forcedValue, defaultValue string,
	sourceChain cli.ValueSourceChain,
	normalize func(string) string,
	currentValue func(*cli.Command) string,
) cli.ValueSourceChain {
	return customModeSourceChain(
		"airgapped",
		isAirgappedModeEnabled,
		&airgappedOverrideRules,
		flagName,
		configKey,
		forcedValue,
		defaultValue,
		sourceChain,
		normalize,
		currentValue,
	)
}

func defaultAirgappedStringSourceChain(
	flagName, envVar, yamlPath, forcedValue, defaultValue string,
) cli.ValueSourceChain {
	return customAirgappedSourceChain(
		flagName,
		yamlPath,
		forcedValue,
		defaultValue,
		defaultSourceChain(envVar, yamlPath),
		func(value string) string {
			return value
		},
		func(cmd *cli.Command) string {
			return cmd.String(flagName)
		},
	)
}

func defaultAirgappedDurationSourceChain(
	flagName, envVar, yamlPath string,
	forcedValue, defaultValue time.Duration,
) cli.ValueSourceChain {
	return customAirgappedSourceChain(
		flagName,
		yamlPath,
		normalizeDurationValue(forcedValue.String()),
		normalizeDurationValue(defaultValue.String()),
		defaultSourceChain(envVar, yamlPath),
		normalizeDurationValue,
		func(cmd *cli.Command) string {
			return normalizeDurationValue(cmd.Duration(flagName).String())
		},
	)
}

func applyAirgappedOverridesBefore(ctx context.Context, cmd *cli.Command) (context.Context, error) {
	if err := applyAirgappedOverrides(cmd); err != nil {
		return ctx, err
	}

	return ctx, nil
}

func applyAirgappedOverrides(cmd *cli.Command) error {
	return applyModeOverrides(
		cmd,
		isAirgappedModeEnabled(),
		airgappedOverrideRules,
		"airgapped mode",
		logAirgappedOverride,
	)
}

func logAirgappedOverride(rule *modeOverrideRule) {
	logrus.Warnf(
		"airgapped mode sets '%s' to '%s'. Set '%s: %s' explicitly to remove this warning.",
		rule.flagName,
		rule.forcedValue,
		rule.configKey,
		formatConfigValue(rule.forcedValue),
	)
}

func cliFlagProvided(flagName string) bool {
	longName := "--" + flagName
	for _, arg := range currentRunArgs {
		if arg == longName || strings.HasPrefix(arg, longName+"=") {
			return true
		}
	}

	return false
}

func isAirgappedModeEnabled() bool {
	if enabled, found := airgappedFlagValueFromArgs(); found {
		return enabled
	}

	if airgapped {
		return true
	}

	sourceChain := defaultSourceChain("AIRGAPPED", "airgapped")
	value, found := sourceChain.Lookup()
	if !found {
		return false
	}

	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false
	}

	return parsed
}

func airgappedFlagValueFromArgs() (value, found bool) {
	for index := 0; index < len(currentRunArgs); index++ {
		arg := currentRunArgs[index]

		switch {
		case arg == "--airgapped":
			value = true
			found = true
		case arg == "--no-airgapped":
			value = false
			found = true
		case strings.HasPrefix(arg, "--airgapped="):
			parsed, err := strconv.ParseBool(strings.TrimPrefix(arg, "--airgapped="))
			if err != nil {
				continue
			}
			value = parsed
			found = true
		}
	}

	return value, found
}

func formatConfigValue(value string) string {
	if value == "" {
		return `""`
	}

	return value
}

func normalizeDurationValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	parsed, err := time.ParseDuration(trimmed)
	if err != nil {
		return trimmed
	}

	if parsed == 0 {
		return "0"
	}

	return parsed.String()
}

func valueOrDefault(value string) string {
	return value
}
