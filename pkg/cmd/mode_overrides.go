package cmd

import (
	"fmt"

	"github.com/urfave/cli/v3"
)

type modeValueSource struct {
	modeName      string
	isModeEnabled func() bool
	forcedValue   string
	defaultValue  string
	fallback      cli.ValueSourceChain
	normalize     func(string) string
}

type modeOverrideRule struct {
	flagName     string
	configKey    string
	forcedValue  string
	source       *modeValueSource
	currentValue func(*cli.Command) string
}

func (s *modeValueSource) Lookup() (string, bool) {
	if !s.isModeEnabled() {
		return "", false
	}

	if s.rawValue() == s.forcedValue {
		return "", false
	}

	return s.forcedValue, true
}

func (s *modeValueSource) rawValue() string {
	if value, found := s.fallback.Lookup(); found {
		return s.normalizeValue(value)
	}

	return s.normalizeValue(s.defaultValue)
}

func (s *modeValueSource) normalizeValue(value string) string {
	if s.normalize == nil {
		return value
	}

	return s.normalize(value)
}

func (s *modeValueSource) String() string {
	return fmt.Sprintf("%s override %q", s.modeName, s.forcedValue)
}

func (s *modeValueSource) GoString() string {
	return fmt.Sprintf("modeValueSource{modeName:%q forcedValue:%q}", s.modeName, s.forcedValue)
}

func customModeSourceChain(
	modeName string,
	isModeEnabled func() bool,
	rules *[]*modeOverrideRule,
	flagName, configKey, forcedValue, defaultValue string,
	sourceChain cli.ValueSourceChain,
	normalize func(string) string,
	currentValue func(*cli.Command) string,
) cli.ValueSourceChain {
	overrideSource := &modeValueSource{
		modeName:      modeName,
		isModeEnabled: isModeEnabled,
		forcedValue:   normalize(valueOrDefault(forcedValue)),
		defaultValue:  normalize(valueOrDefault(defaultValue)),
		fallback:      sourceChain,
		normalize:     normalize,
	}

	*rules = append(*rules, &modeOverrideRule{
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

func applyModeOverrides(
	cmd *cli.Command,
	enabled bool,
	rules []*modeOverrideRule,
	errorContext string,
	logOverride func(*modeOverrideRule),
) error {
	if !enabled {
		return nil
	}

	for _, rule := range rules {
		currentValue := rule.currentValue(cmd)
		if currentValue != rule.forcedValue {
			if err := cmd.Set(rule.flagName, rule.forcedValue); err != nil {
				return fmt.Errorf("set %s for %s: %w", rule.flagName, errorContext, err)
			}
			logOverride(rule)
			continue
		}

		if cliFlagProvided(rule.flagName) {
			continue
		}

		if rule.source.rawValue() != rule.forcedValue {
			logOverride(rule)
		}
	}

	return nil
}
