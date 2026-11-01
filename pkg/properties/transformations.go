package properties

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// TransformationsConfig contains ordered early-event rules, without editor metadata.
type TransformationsConfig struct {
	Rules []TransformationRuleConfig `yaml:"rules" json:"rules"`
}

// TransformationRuleConfig matches the current event before applying its actions.
type TransformationRuleConfig struct {
	Name       string                       `yaml:"name" json:"name"`
	Expression string                       `yaml:"expression" json:"expression"`
	Actions    []TransformationActionConfig `yaml:"actions" json:"actions"`
}

// TransformationFieldReference selects exactly one of: a catalog field, a typed custom
// event parameter, or a query parameter of the page URL.
type TransformationFieldReference struct {
	Field        string `yaml:"field,omitempty" json:"field,omitempty"`
	Parameter    string `yaml:"parameter,omitempty" json:"parameter,omitempty"`
	URLParameter string `yaml:"url_parameter,omitempty" json:"url_parameter,omitempty"`
	Type         string `yaml:"type,omitempty" json:"type,omitempty"`
}

// TransformationLiteral is exactly one of a string, a number (including zero) or a
// template such as "{{page_title}} | {{params.section}}".
type TransformationLiteral struct {
	String   *string  `yaml:"string,omitempty" json:"string,omitempty"`
	Number   *float64 `yaml:"number,omitempty" json:"number,omitempty"`
	Template *string  `yaml:"template,omitempty" json:"template,omitempty"`
}

// TransformationActionType is the kind of a transformation action.
type TransformationActionType string

const (
	// TransformationActionSet sets the target to a literal or template value.
	TransformationActionSet TransformationActionType = "set"
	// TransformationActionRemove removes the target.
	TransformationActionRemove TransformationActionType = "remove"
	// TransformationActionReplace replaces pattern matches in the target.
	TransformationActionReplace TransformationActionType = "replace"
	// TransformationActionDrop discards the event; it must be the rule's only action.
	TransformationActionDrop TransformationActionType = "drop"
)

// TransformationActionConfig describes a set, remove, replace or exclusive drop action.
type TransformationActionConfig struct {
	Type   TransformationActionType      `yaml:"type" json:"type"`
	Target *TransformationFieldReference `yaml:"target,omitempty" json:"target,omitempty"`
	Value  *TransformationLiteral        `yaml:"value,omitempty" json:"value,omitempty"`
	// Pattern is the regular expression a replace action searches for (RE2 syntax).
	Pattern string `yaml:"pattern,omitempty" json:"pattern,omitempty"`
	// Replacement replaces every match; nil means "[REDACTED]" and "" removes matches.
	// $1 or ${name} insert capture groups; $$ is a literal dollar sign.
	Replacement *string `yaml:"replacement,omitempty" json:"replacement,omitempty"`
}

// ParseTransformationsConfig reads only the transformations section, strictly checking its keys.
func ParseTransformationsConfig(path string) (TransformationsConfig, error) {
	content, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return TransformationsConfig{}, fmt.Errorf("reading transformations config: %w", err)
	}
	var root struct {
		Transformations yaml.Node `yaml:"transformations"`
	}
	if err := yaml.Unmarshal(content, &root); err != nil {
		return TransformationsConfig{}, fmt.Errorf("reading transformations YAML: %w", err)
	}
	var config TransformationsConfig
	if root.Transformations.Kind == 0 {
		return config, nil
	}
	section, err := yaml.Marshal(&root.Transformations)
	if err != nil {
		return config, fmt.Errorf("encoding transformations section: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(section))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return config, fmt.Errorf("decoding transformations section: %w", err)
	}
	return config, nil
}
