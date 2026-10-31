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

// TransformationFieldReference selects either a friendly field or a typed custom parameter.
type TransformationFieldReference struct {
	Field     string `yaml:"field,omitempty" json:"field,omitempty"`
	Parameter string `yaml:"parameter,omitempty" json:"parameter,omitempty"`
	Type      string `yaml:"type,omitempty" json:"type,omitempty"`
}

// TransformationLiteral preserves the distinction between strings and numbers, including zero.
type TransformationLiteral struct {
	String *string  `yaml:"string,omitempty" json:"string,omitempty"`
	Number *float64 `yaml:"number,omitempty" json:"number,omitempty"`
}

// TransformationActionConfig describes a set, copy, remove or exclusive drop action.
type TransformationActionConfig struct {
	Type   string                        `yaml:"type" json:"type"`
	Target *TransformationFieldReference `yaml:"target,omitempty" json:"target,omitempty"`
	Value  *TransformationLiteral        `yaml:"value,omitempty" json:"value,omitempty"`
	Source *TransformationFieldReference `yaml:"source,omitempty" json:"source,omitempty"`
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
