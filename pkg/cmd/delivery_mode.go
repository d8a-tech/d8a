package cmd

import (
	"context"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

const deliveryModeAtLeastOnce = "at_least_once"

var deliveryModeOverrideRules []*modeOverrideRule

func customDeliveryModeSourceChain(
	flagName, configKey, forcedValue, defaultValue string,
	sourceChain cli.ValueSourceChain,
	normalize func(string) string,
	currentValue func(*cli.Command) string,
) cli.ValueSourceChain {
	return customModeSourceChain(
		"delivery mode",
		isAtLeastOnceDeliveryModeEnabled,
		&deliveryModeOverrideRules,
		flagName,
		configKey,
		forcedValue,
		defaultValue,
		sourceChain,
		normalize,
		currentValue,
	)
}

func defaultDeliveryModeStringSourceChain(
	flagName, envVar, yamlPath, forcedValue, defaultValue string,
) cli.ValueSourceChain {
	return customDeliveryModeSourceChain(
		flagName,
		yamlPath,
		forcedValue,
		defaultValue,
		defaultSourceChain(envVar, yamlPath),
		normalizeStringValue,
		func(cmd *cli.Command) string {
			return normalizeStringValue(cmd.String(flagName))
		},
	)
}

func defaultDeliveryModeBoolSourceChain(
	flagName, envVar, yamlPath string,
	forcedValue, defaultValue bool,
) cli.ValueSourceChain {
	return customDeliveryModeSourceChain(
		flagName,
		yamlPath,
		strconv.FormatBool(forcedValue),
		strconv.FormatBool(defaultValue),
		defaultSourceChain(envVar, yamlPath),
		normalizeBoolValue,
		func(cmd *cli.Command) string {
			return normalizeBoolValue(strconv.FormatBool(cmd.Bool(flagName)))
		},
	)
}

func defaultDeliveryModeIntSourceChain(
	flagName, envVar, yamlPath string,
	forcedValue, defaultValue int,
) cli.ValueSourceChain {
	return customDeliveryModeSourceChain(
		flagName,
		yamlPath,
		strconv.Itoa(forcedValue),
		strconv.Itoa(defaultValue),
		defaultSourceChain(envVar, yamlPath),
		normalizeIntValue,
		func(cmd *cli.Command) string {
			return normalizeIntValue(strconv.Itoa(cmd.Int(flagName)))
		},
	)
}

func applyDeliveryModeOverridesBefore(ctx context.Context, cmd *cli.Command) (context.Context, error) {
	if err := applyDeliveryModeOverrides(cmd); err != nil {
		return ctx, err
	}

	return ctx, nil
}

func applyDeliveryModeOverrides(cmd *cli.Command) error {
	return applyModeOverrides(
		cmd,
		isAtLeastOnceDeliveryModeEnabled(),
		deliveryModeOverrideRules,
		"delivery mode at_least_once",
		logDeliveryModeOverride,
	)
}

func logDeliveryModeOverride(rule *modeOverrideRule) {
	logrus.Warnf(
		"delivery mode 'at_least_once' sets '%s' to '%s'. Set '%s: %s' explicitly to remove this warning.",
		rule.flagName,
		rule.forcedValue,
		rule.configKey,
		formatConfigValue(rule.forcedValue),
	)
}

func isAtLeastOnceDeliveryModeEnabled() bool {
	if mode, found := deliveryModeValueFromArgs(); found {
		return mode == deliveryModeAtLeastOnce
	}

	if normalizeStringValue(deliveryMode) == deliveryModeAtLeastOnce {
		return true
	}

	sourceChain := defaultSourceChain("DELIVERY_MODE", "delivery.mode")
	value, found := sourceChain.Lookup()
	if !found {
		return false
	}

	return normalizeStringValue(value) == deliveryModeAtLeastOnce
}

func deliveryModeValueFromArgs() (value string, found bool) {
	for index := 0; index < len(currentRunArgs); index++ {
		arg := currentRunArgs[index]

		switch {
		case strings.HasPrefix(arg, "--delivery-mode="):
			value = strings.TrimPrefix(arg, "--delivery-mode=")
			found = true
		case arg == "--delivery-mode":
			if index+1 >= len(currentRunArgs) {
				continue
			}
			value = currentRunArgs[index+1]
			found = true
		}
	}

	return value, found
}

func normalizeStringValue(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}

func normalizeBoolValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	parsed, err := strconv.ParseBool(trimmed)
	if err != nil {
		return trimmed
	}

	return strconv.FormatBool(parsed)
}

func normalizeIntValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return trimmed
	}

	return strconv.Itoa(parsed)
}
