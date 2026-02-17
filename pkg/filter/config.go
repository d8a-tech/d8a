// Package filter provides event filtering functionality.
package filter

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// FilterType indicates the type of filter (exclude or allow).
type FilterType string

const (
	// FilterTypeExclude removes matching events from sessions.
	FilterTypeExclude FilterType = "exclude"
	// FilterTypeAllow removes non-matching events from sessions.
	FilterTypeAllow FilterType = "allow"
)

// ConditionConfig defines a single filter condition.
type ConditionConfig struct {
	Name       string     `yaml:"name"`
	Type       FilterType `yaml:"type"`
	Active     bool       `yaml:"active"`
	Expression string     `yaml:"expression"`
}

// FiltersConfig defines the complete filters configuration.
type FiltersConfig struct {
	Fields     []string          `yaml:"fields"`
	Conditions []ConditionConfig `yaml:"conditions"`
}

// ParseConfig reads the filters section from a YAML config file.
func ParseConfig(configFilePath string) (FiltersConfig, error) {
	// nolint:gosec // configFilePath comes from CLI, not user input
	content, err := os.ReadFile(configFilePath)
	if err != nil {
		return FiltersConfig{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var rawConfig struct {
		Filters FiltersConfig `yaml:"filters"`
	}
	if err := yaml.Unmarshal(content, &rawConfig); err != nil {
		return FiltersConfig{}, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	return rawConfig.Filters, nil
}
