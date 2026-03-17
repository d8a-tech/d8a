package properties

import (
	"fmt"
	"os"

	"github.com/d8a-tech/d8a/pkg/schema"
	"gopkg.in/yaml.v3"
)

// ProtocolCustomColumnsConfig holds shortcut custom-column definitions under protocol YAML subtree.
type ProtocolCustomColumnsConfig struct {
	GA4Params              []GA4ParamShortcutConfig              `yaml:"ga4_params"`
	MatomoCustomDimensions []MatomoCustomDimensionShortcutConfig `yaml:"matomo_custom_dimensions"`
	MatomoCustomVariables  []MatomoCustomVariableShortcutConfig  `yaml:"matomo_custom_variables"`
}

// GA4ParamShortcutConfig defines one protocol.ga4_params entry.
type GA4ParamShortcutConfig struct {
	Name  string            `yaml:"name"`
	Scope CustomColumnScope `yaml:"scope"`
	Type  CustomColumnType  `yaml:"type"`
}

// MatomoCustomDimensionShortcutConfig defines one protocol.matomo_custom_dimensions entry.
type MatomoCustomDimensionShortcutConfig struct {
	Slot  int64             `yaml:"slot"`
	Name  string            `yaml:"name"`
	Scope CustomColumnScope `yaml:"scope"`
	Type  CustomColumnType  `yaml:"type"`
}

// MatomoCustomVariableShortcutConfig defines one protocol.matomo_custom_variables entry.
type MatomoCustomVariableShortcutConfig struct {
	Name  string            `yaml:"name"`
	Scope CustomColumnScope `yaml:"scope"`
	Type  CustomColumnType  `yaml:"type"`
}

// CustomColumnScope defines where the resulting column is written.
type CustomColumnScope string

const (
	CustomColumnScopeEvent              CustomColumnScope = "event"
	CustomColumnScopeSession            CustomColumnScope = "session"
	CustomColumnScopeSessionScopedEvent CustomColumnScope = "session_scoped_event"
)

// CustomColumnType defines output type for the resulting column.
type CustomColumnType string

const (
	CustomColumnTypeString  CustomColumnType = "string"
	CustomColumnTypeInt64   CustomColumnType = "int64"
	CustomColumnTypeFloat64 CustomColumnType = "float64"
	CustomColumnTypeBool    CustomColumnType = "bool"
)

// NestedLookupPickStrategy defines deterministic value-picking strategy for repeated sources.
type NestedLookupPickStrategy string

const (
	NestedLookupPickStrategyLastNonNull NestedLookupPickStrategy = "last_non_null"
)

// NestedLookupConfig stores normalized nested source lookup details.
type NestedLookupConfig struct {
	SourceInterfaceID schema.InterfaceID
	SourceField       string
	MatchField        string
	MatchEquals       any
	ValueField        string
	Pick              NestedLookupPickStrategy
}

// CustomColumnConfig is normalized custom-column config used by runtime builders.
type CustomColumnConfig struct {
	Name           string
	Scope          CustomColumnScope
	Type           CustomColumnType
	DependsOn      schema.DependsOnEntry
	Implementation NestedLookupConfig
}

// ProtocolCustomColumnsParser parses protocol shortcut custom columns from YAML file.
type ProtocolCustomColumnsParser interface {
	Parse(configFilePath string) (ProtocolCustomColumnsConfig, error)
}

type protocolCustomColumnsParser struct{}

// NewProtocolCustomColumnsParser creates a parser for protocol shortcut custom columns.
func NewProtocolCustomColumnsParser() ProtocolCustomColumnsParser {
	return protocolCustomColumnsParser{}
}

// ParseProtocolCustomColumnsConfig reads protocol custom column shortcuts from YAML config.
func ParseProtocolCustomColumnsConfig(configFilePath string) (ProtocolCustomColumnsConfig, error) {
	return NewProtocolCustomColumnsParser().Parse(configFilePath)
}

func (p protocolCustomColumnsParser) Parse(configFilePath string) (ProtocolCustomColumnsConfig, error) {
	// nolint:gosec // configFilePath comes from CLI, not user input
	content, err := os.ReadFile(configFilePath)
	if err != nil {
		return ProtocolCustomColumnsConfig{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var rawConfig struct {
		Protocol ProtocolCustomColumnsConfig `yaml:"protocol"`
	}
	if err := yaml.Unmarshal(content, &rawConfig); err != nil {
		return ProtocolCustomColumnsConfig{}, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	return rawConfig.Protocol, nil
}

// CustomColumnNormalizer normalizes shortcut definitions into internal custom-column DTOs.
type CustomColumnNormalizer interface {
	Normalize(shortcuts ProtocolCustomColumnsConfig) ([]CustomColumnConfig, error)
}

type customColumnNormalizer struct {
	validator CustomColumnValidator
}

// NewCustomColumnNormalizer creates a shortcut normalizer.
func NewCustomColumnNormalizer(validator CustomColumnValidator) CustomColumnNormalizer {
	return customColumnNormalizer{validator: validator}
}

// CustomColumnValidator validates normalized custom-column DTOs.
type CustomColumnValidator interface {
	Validate(columns []CustomColumnConfig) error
}

type customColumnValidator struct{}

// NewCustomColumnValidator creates normalized custom-column validator.
func NewCustomColumnValidator() CustomColumnValidator {
	return customColumnValidator{}
}

func (v customColumnValidator) Validate(columns []CustomColumnConfig) error {
	names := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if _, exists := names[column.Name]; exists {
			return fmt.Errorf("duplicate custom column output name %q", column.Name)
		}
		names[column.Name] = struct{}{}
	}

	return nil
}

func (n customColumnNormalizer) Normalize(shortcuts ProtocolCustomColumnsConfig) ([]CustomColumnConfig, error) {
	columns := make([]CustomColumnConfig, 0, len(shortcuts.GA4Params)+len(shortcuts.MatomoCustomDimensions)+len(shortcuts.MatomoCustomVariables))

	for idx, entry := range shortcuts.GA4Params {
		column, err := normalizeGA4ParamShortcut(entry, idx)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	for idx, entry := range shortcuts.MatomoCustomDimensions {
		column, err := normalizeMatomoCustomDimensionShortcut(entry, idx)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	for idx, entry := range shortcuts.MatomoCustomVariables {
		column, err := normalizeMatomoCustomVariableShortcut(entry, idx)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	if err := n.validator.Validate(columns); err != nil {
		return nil, err
	}

	return columns, nil
}

func normalizeGA4ParamShortcut(entry GA4ParamShortcutConfig, idx int) (CustomColumnConfig, error) {
	pathPrefix := fmt.Sprintf("protocol.ga4_params[%d]", idx)
	if entry.Name == "" {
		return CustomColumnConfig{}, fmt.Errorf("%s.name is required", pathPrefix)
	}

	scope := defaultScope(entry.Scope)
	if scope != CustomColumnScopeEvent {
		return CustomColumnConfig{}, fmt.Errorf("%s.scope must be %q for GA4 params", pathPrefix, CustomColumnScopeEvent)
	}

	columnType := defaultType(entry.Type)
	if err := validateType(pathPrefix+".type", columnType); err != nil {
		return CustomColumnConfig{}, err
	}
	if columnType == CustomColumnTypeBool {
		return CustomColumnConfig{}, fmt.Errorf("%s.type %q is unsupported for protocol.ga4_params", pathPrefix, columnType)
	}

	valueField := "value_string"
	if columnType == CustomColumnTypeFloat64 || columnType == CustomColumnTypeInt64 {
		valueField = "value_number"
	}

	return CustomColumnConfig{
		Name:      entry.Name,
		Scope:     scope,
		Type:      columnType,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("ga4.protocols.d8a.tech/event/params")},
		Implementation: NestedLookupConfig{
			SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
			SourceField:       "params",
			MatchField:        "name",
			MatchEquals:       entry.Name,
			ValueField:        valueField,
		},
	}, nil
}

func normalizeMatomoCustomDimensionShortcut(entry MatomoCustomDimensionShortcutConfig, idx int) (CustomColumnConfig, error) {
	pathPrefix := fmt.Sprintf("protocol.matomo_custom_dimensions[%d]", idx)
	if entry.Name == "" {
		return CustomColumnConfig{}, fmt.Errorf("%s.name is required", pathPrefix)
	}
	if entry.Slot == 0 {
		return CustomColumnConfig{}, fmt.Errorf("%s.slot is required", pathPrefix)
	}

	scope := defaultScope(entry.Scope)
	columnType := defaultType(entry.Type)
	if err := validateType(pathPrefix+".type", columnType); err != nil {
		return CustomColumnConfig{}, err
	}
	if columnType != CustomColumnTypeString {
		return CustomColumnConfig{}, fmt.Errorf("%s.type %q is unsupported; Matomo custom dimensions can only use type %q (value field is always a string)", pathPrefix, columnType, CustomColumnTypeString)
	}

	implementation := NestedLookupConfig{
		MatchField:  "slot",
		MatchEquals: entry.Slot,
		ValueField:  "value",
	}

	dependsOnID, sourceField, err := matomoLookupByScope(pathPrefix+".scope", scope, "custom_dimensions", "session_custom_dimensions")
	if err != nil {
		return CustomColumnConfig{}, err
	}
	implementation.SourceInterfaceID = dependsOnID
	implementation.SourceField = sourceField
	if scope != CustomColumnScopeEvent {
		implementation.Pick = NestedLookupPickStrategyLastNonNull
	}

	return CustomColumnConfig{
		Name:           entry.Name,
		Scope:          scope,
		Type:           columnType,
		DependsOn:      schema.DependsOnEntry{Interface: dependsOnID},
		Implementation: implementation,
	}, nil
}

func normalizeMatomoCustomVariableShortcut(entry MatomoCustomVariableShortcutConfig, idx int) (CustomColumnConfig, error) {
	pathPrefix := fmt.Sprintf("protocol.matomo_custom_variables[%d]", idx)
	if entry.Name == "" {
		return CustomColumnConfig{}, fmt.Errorf("%s.name is required", pathPrefix)
	}

	scope := defaultScope(entry.Scope)
	columnType := defaultType(entry.Type)
	if err := validateType(pathPrefix+".type", columnType); err != nil {
		return CustomColumnConfig{}, err
	}
	if columnType != CustomColumnTypeString {
		return CustomColumnConfig{}, fmt.Errorf("%s.type %q is unsupported; Matomo custom variables can only use type %q (value field is always a string)", pathPrefix, columnType, CustomColumnTypeString)
	}

	dependsOnID, sourceField, err := matomoLookupByScope(pathPrefix+".scope", scope, "custom_variables", "session_custom_variables")
	if err != nil {
		return CustomColumnConfig{}, err
	}

	implementation := NestedLookupConfig{
		SourceInterfaceID: dependsOnID,
		SourceField:       sourceField,
		MatchField:        "name",
		MatchEquals:       entry.Name,
		ValueField:        "value",
	}
	if scope != CustomColumnScopeEvent {
		implementation.Pick = NestedLookupPickStrategyLastNonNull
	}

	return CustomColumnConfig{
		Name:           entry.Name,
		Scope:          scope,
		Type:           columnType,
		DependsOn:      schema.DependsOnEntry{Interface: dependsOnID},
		Implementation: implementation,
	}, nil
}

func matomoLookupByScope(path string, scope CustomColumnScope, eventSourceField, sessionSourceField string) (schema.InterfaceID, string, error) {
	switch scope {
	case CustomColumnScopeEvent:
		if eventSourceField == "custom_dimensions" {
			return schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_dimensions"), eventSourceField, nil
		}
		return schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_variables"), eventSourceField, nil
	case CustomColumnScopeSession:
		if sessionSourceField == "session_custom_dimensions" {
			return schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_dimensions"), sessionSourceField, nil
		}
		return schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables"), sessionSourceField, nil
	default:
		return "", "", fmt.Errorf("%s has invalid value %q", path, scope)
	}
}

func defaultScope(scope CustomColumnScope) CustomColumnScope {
	if scope == "" {
		return CustomColumnScopeEvent
	}
	return scope
}

func defaultType(columnType CustomColumnType) CustomColumnType {
	if columnType == "" {
		return CustomColumnTypeString
	}
	return columnType
}

func validateType(path string, columnType CustomColumnType) error {
	switch columnType {
	case CustomColumnTypeString, CustomColumnTypeInt64, CustomColumnTypeFloat64, CustomColumnTypeBool:
		return nil
	default:
		return fmt.Errorf("%s has invalid value %q", path, columnType)
	}
}
