package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

var airgapped bool

var currentRunArgs []string

type airgappedValueSource struct {
	airgapped    *bool
	forcedValue  string
	defaultValue string
	fallback     cli.ValueSourceChain
	normalize    func(string) string
}

type airgappedOverrideRule struct {
	flagName     string
	configKey    string
	forcedValue  string
	source       *airgappedValueSource
	currentValue func(*cli.Command) string
}

var airgappedOverrideRules []*airgappedOverrideRule

func (s *airgappedValueSource) Lookup() (string, bool) {
	if !isAirgappedModeEnabled() {
		return "", false
	}

	if s.rawValue() == s.forcedValue {
		return "", false
	}

	return s.forcedValue, true
}

func (s *airgappedValueSource) rawValue() string {
	if value, found := s.fallback.Lookup(); found {
		return s.normalizeValue(value)
	}

	return s.normalizeValue(s.defaultValue)
}

func (s *airgappedValueSource) normalizeValue(value string) string {
	if s.normalize == nil {
		return value
	}

	return s.normalize(value)
}

func (s *airgappedValueSource) String() string {
	return fmt.Sprintf("airgapped override %q", s.forcedValue)
}

func (s *airgappedValueSource) GoString() string {
	return fmt.Sprintf("airgappedValueSource{forcedValue:%q}", s.forcedValue)
}

func customAirgappedSourceChain(
	flagName, configKey, forcedValue, defaultValue string,
	sourceChain cli.ValueSourceChain,
	normalize func(string) string,
	currentValue func(*cli.Command) string,
) cli.ValueSourceChain {
	overrideSource := &airgappedValueSource{
		airgapped:    &airgapped,
		forcedValue:  normalize(valueOrDefault(forcedValue)),
		defaultValue: normalize(valueOrDefault(defaultValue)),
		fallback:     sourceChain,
		normalize:    normalize,
	}

	airgappedOverrideRules = append(airgappedOverrideRules, &airgappedOverrideRule{
		flagName:     flagName,
		configKey:    configKey,
		forcedValue:  overrideSource.forcedValue,
		source:       overrideSource,
		currentValue: currentValue,
	})

	combined := cli.NewValueSourceChain(overrideSource)
	combined.Append(sourceChain)
	return combined
}

func defaultAirgappedBoolSourceChain(
	flagName, envVar, yamlPath string,
	forcedValue, defaultValue bool,
) cli.ValueSourceChain {
	return customAirgappedSourceChain(
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
	if !isAirgappedModeEnabled() {
		return nil
	}

	for _, rule := range airgappedOverrideRules {
		currentValue := rule.currentValue(cmd)
		if currentValue != rule.forcedValue {
			if err := cmd.Set(rule.flagName, rule.forcedValue); err != nil {
				return fmt.Errorf("set %s for airgapped mode: %w", rule.flagName, err)
			}
			logAirgappedOverride(rule)
			continue
		}

		if cliFlagProvided(rule.flagName) {
			continue
		}

		if rule.source.rawValue() != rule.forcedValue {
			logAirgappedOverride(rule)
		}
	}

	return nil
}

func logAirgappedOverride(rule *airgappedOverrideRule) {
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

func normalizeBoolValue(value string) string {
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return strings.TrimSpace(value)
	}

	return strconv.FormatBool(parsed)
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
