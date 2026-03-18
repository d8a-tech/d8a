package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type protocolCustomColumnsConfig struct {
	GA4Params              []ga4ParamShortcutConfig              `yaml:"ga4_params"`
	MatomoCustomDimensions []matomoCustomDimensionShortcutConfig `yaml:"matomo_custom_dimensions"`
	MatomoCustomVariables  []matomoCustomVariableShortcutConfig  `yaml:"matomo_custom_variables"`
}

type ga4ParamShortcutConfig struct {
	Name  string                       `yaml:"name" json:"name"`
	Scope properties.CustomColumnScope `yaml:"scope" json:"scope"`
	Type  properties.CustomColumnType  `yaml:"type" json:"type"`
}

type matomoCustomDimensionShortcutConfig struct {
	Slot  int64                        `yaml:"slot" json:"slot"`
	Name  string                       `yaml:"name" json:"name"`
	Scope properties.CustomColumnScope `yaml:"scope" json:"scope"`
	Type  properties.CustomColumnType  `yaml:"type" json:"type"`
}

type matomoCustomVariableShortcutConfig struct {
	Name  string                       `yaml:"name" json:"name"`
	Scope properties.CustomColumnScope `yaml:"scope" json:"scope"`
	Type  properties.CustomColumnType  `yaml:"type" json:"type"`
}

type protocolCustomColumnsParser interface {
	Parse(configFilePath string) (protocolCustomColumnsConfig, error)
}

type yamlProtocolCustomColumnsParser struct{}

func newProtocolCustomColumnsParser() protocolCustomColumnsParser {
	return yamlProtocolCustomColumnsParser{}
}

func parseProtocolCustomColumnsConfig(configFilePath string) (protocolCustomColumnsConfig, error) {
	return newProtocolCustomColumnsParser().Parse(configFilePath)
}

func (p yamlProtocolCustomColumnsParser) Parse(configFilePath string) (protocolCustomColumnsConfig, error) {
	// nolint:gosec // configFilePath comes from CLI, not user input
	content, err := os.ReadFile(configFilePath)
	if err != nil {
		return protocolCustomColumnsConfig{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var rawConfig struct {
		Protocol protocolCustomColumnsConfig `yaml:"protocol"`
	}
	if err := yaml.Unmarshal(content, &rawConfig); err != nil {
		return protocolCustomColumnsConfig{}, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	return rawConfig.Protocol, nil
}

type protocolCustomColumnNormalizer interface {
	Normalize(shortcuts protocolCustomColumnsConfig) ([]properties.CustomColumnConfig, error)
}

type shortcutCustomColumnNormalizer struct {
	validator protocolCustomColumnValidator
}

func newProtocolCustomColumnNormalizer(validator protocolCustomColumnValidator) protocolCustomColumnNormalizer {
	return shortcutCustomColumnNormalizer{validator: validator}
}

type protocolCustomColumnValidator interface {
	Validate(columns []properties.CustomColumnConfig) error
}

type uniqueNameCustomColumnValidator struct{}

func newProtocolCustomColumnValidator() protocolCustomColumnValidator {
	return uniqueNameCustomColumnValidator{}
}

func (v uniqueNameCustomColumnValidator) Validate(columns []properties.CustomColumnConfig) error {
	names := make(map[string]struct{}, len(columns))
	for idx := range columns {
		name := columns[idx].Name
		if _, exists := names[name]; exists {
			return fmt.Errorf("duplicate custom column output name %q", name)
		}
		names[name] = struct{}{}
	}

	return nil
}

func (n shortcutCustomColumnNormalizer) Normalize(
	shortcuts protocolCustomColumnsConfig,
) ([]properties.CustomColumnConfig, error) {
	totalColumns := len(shortcuts.GA4Params) +
		len(shortcuts.MatomoCustomDimensions) +
		len(shortcuts.MatomoCustomVariables)

	columns := make([]properties.CustomColumnConfig, 0, totalColumns)

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

func loadProtocolCustomColumns(cmd protocolCustomColumnsSource) ([]properties.CustomColumnConfig, error) {
	shortcuts := protocolCustomColumnsConfig{}

	if _, err := os.Stat(configFile); err == nil {
		parsed, parseErr := parseProtocolCustomColumnsConfig(configFile)
		if parseErr != nil {
			return nil, parseErr
		}
		shortcuts = parsed
	}

	appendJSONShortcutEntries(&shortcuts, cmd)

	normalizer := newProtocolCustomColumnNormalizer(newProtocolCustomColumnValidator())
	return normalizer.Normalize(shortcuts)
}

type protocolCustomColumnsSource interface {
	StringSlice(name string) []string
}

func appendJSONShortcutEntries(shortcuts *protocolCustomColumnsConfig, cmd protocolCustomColumnsSource) {
	decodeAndAppendJSONEntries(
		cmd.StringSlice(protocolGA4ParamsFlag.Name),
		"protocol-ga4-params",
		func(entry ga4ParamShortcutConfig) {
			shortcuts.GA4Params = append(shortcuts.GA4Params, entry)
		},
	)

	decodeAndAppendJSONEntries(
		cmd.StringSlice(protocolMatomoCustomDimensionsFlag.Name),
		"protocol-matomo-custom-dimensions",
		func(entry matomoCustomDimensionShortcutConfig) {
			shortcuts.MatomoCustomDimensions = append(shortcuts.MatomoCustomDimensions, entry)
		},
	)

	decodeAndAppendJSONEntries(
		cmd.StringSlice(protocolMatomoCustomVariablesFlag.Name),
		"protocol-matomo-custom-variables",
		func(entry matomoCustomVariableShortcutConfig) {
			shortcuts.MatomoCustomVariables = append(shortcuts.MatomoCustomVariables, entry)
		},
	)
}

func decodeAndAppendJSONEntries[T any](entries []string, flagName string, appendFn func(T)) {
	for _, entryJSON := range entries {
		var entry T
		if err := json.Unmarshal([]byte(entryJSON), &entry); err != nil {
			logrus.Warnf("skipping invalid JSON %s entry %q: %v", flagName, entryJSON, err)
			continue
		}
		appendFn(entry)
	}
}

func normalizeGA4ParamShortcut(entry ga4ParamShortcutConfig, idx int) (properties.CustomColumnConfig, error) {
	pathPrefix := fmt.Sprintf("protocol.ga4_params[%d]", idx)
	if entry.Name == "" {
		return properties.CustomColumnConfig{}, fmt.Errorf("%s.name is required", pathPrefix)
	}

	scope := defaultScope(entry.Scope)
	if scope != properties.CustomColumnScopeEvent {
		return properties.CustomColumnConfig{}, fmt.Errorf(
			"%s.scope must be %q for GA4 params",
			pathPrefix,
			properties.CustomColumnScopeEvent,
		)
	}

	columnType := defaultType(entry.Type)
	if err := validateType(pathPrefix+".type", columnType); err != nil {
		return properties.CustomColumnConfig{}, err
	}
	if columnType == properties.CustomColumnTypeBool {
		return properties.CustomColumnConfig{}, fmt.Errorf(
			"%s.type %q is unsupported for protocol.ga4_params",
			pathPrefix,
			columnType,
		)
	}

	valueField := "value_string"
	if columnType == properties.CustomColumnTypeFloat64 || columnType == properties.CustomColumnTypeInt64 {
		valueField = "value_number"
	}

	return properties.CustomColumnConfig{
		Name:      entry.Name,
		Scope:     scope,
		Type:      columnType,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("ga4.protocols.d8a.tech/event/params")},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
			SourceField:       "params",
			MatchField:        "name",
			MatchEquals:       entry.Name,
			ValueField:        valueField,
		},
	}, nil
}

func normalizeMatomoCustomDimensionShortcut(
	entry matomoCustomDimensionShortcutConfig,
	idx int,
) (properties.CustomColumnConfig, error) {
	pathPrefix := fmt.Sprintf("protocol.matomo_custom_dimensions[%d]", idx)
	if entry.Name == "" {
		return properties.CustomColumnConfig{}, fmt.Errorf("%s.name is required", pathPrefix)
	}
	if entry.Slot == 0 {
		return properties.CustomColumnConfig{}, fmt.Errorf("%s.slot is required", pathPrefix)
	}

	scope := defaultScope(entry.Scope)
	columnType := defaultType(entry.Type)
	if err := validateType(pathPrefix+".type", columnType); err != nil {
		return properties.CustomColumnConfig{}, err
	}
	if columnType != properties.CustomColumnTypeString {
		return properties.CustomColumnConfig{}, fmt.Errorf(
			"%s.type %q is unsupported; Matomo custom dimensions can only use type %q "+
				"(value field is always a string)",
			pathPrefix,
			columnType,
			properties.CustomColumnTypeString,
		)
	}

	implementation := properties.NestedLookupConfig{
		SourceScope: properties.NestedLookupSourceScopeEvent,
		MatchField:  "slot",
		MatchEquals: entry.Slot,
		ValueField:  "value",
	}

	dependsOnID, sourceField, err := matomoLookupByScope(
		pathPrefix+".scope",
		scope,
		"custom_dimensions",
		"session_custom_dimensions",
	)
	if err != nil {
		return properties.CustomColumnConfig{}, err
	}
	implementation.SourceInterfaceID = dependsOnID
	implementation.SourceField = sourceField
	if dependsOnID == schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_dimensions") {
		implementation.SourceScope = properties.NestedLookupSourceScopeSession
	}
	if scope != properties.CustomColumnScopeEvent {
		implementation.Pick = properties.NestedLookupPickStrategyLastNonNull
	}

	return properties.CustomColumnConfig{
		Name:           entry.Name,
		Scope:          scope,
		Type:           columnType,
		DependsOn:      schema.DependsOnEntry{Interface: dependsOnID},
		Implementation: implementation,
	}, nil
}

func normalizeMatomoCustomVariableShortcut(
	entry matomoCustomVariableShortcutConfig,
	idx int,
) (properties.CustomColumnConfig, error) {
	pathPrefix := fmt.Sprintf("protocol.matomo_custom_variables[%d]", idx)
	if entry.Name == "" {
		return properties.CustomColumnConfig{}, fmt.Errorf("%s.name is required", pathPrefix)
	}

	scope := defaultScope(entry.Scope)
	columnType := defaultType(entry.Type)
	if err := validateType(pathPrefix+".type", columnType); err != nil {
		return properties.CustomColumnConfig{}, err
	}
	if columnType != properties.CustomColumnTypeString {
		return properties.CustomColumnConfig{}, fmt.Errorf(
			"%s.type %q is unsupported; Matomo custom variables can only use type %q "+
				"(value field is always a string)",
			pathPrefix,
			columnType,
			properties.CustomColumnTypeString,
		)
	}

	dependsOnID, sourceField, err := matomoLookupByScope(
		pathPrefix+".scope",
		scope,
		"custom_variables",
		"session_custom_variables",
	)
	if err != nil {
		return properties.CustomColumnConfig{}, err
	}

	implementation := properties.NestedLookupConfig{
		SourceScope:       properties.NestedLookupSourceScopeEvent,
		SourceInterfaceID: dependsOnID,
		SourceField:       sourceField,
		MatchField:        "name",
		MatchEquals:       entry.Name,
		ValueField:        "value",
	}
	if dependsOnID == schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables") {
		implementation.SourceScope = properties.NestedLookupSourceScopeSession
	}
	if scope != properties.CustomColumnScopeEvent {
		implementation.Pick = properties.NestedLookupPickStrategyLastNonNull
	}

	return properties.CustomColumnConfig{
		Name:           entry.Name,
		Scope:          scope,
		Type:           columnType,
		DependsOn:      schema.DependsOnEntry{Interface: dependsOnID},
		Implementation: implementation,
	}, nil
}

func matomoLookupByScope(
	path string,
	scope properties.CustomColumnScope,
	eventSourceField string,
	sessionSourceField string,
) (schema.InterfaceID, string, error) {
	switch scope {
	case properties.CustomColumnScopeEvent:
		if eventSourceField == "custom_dimensions" {
			return schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_dimensions"), eventSourceField, nil
		}
		return schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_variables"), eventSourceField, nil
	case properties.CustomColumnScopeSession:
		if sessionSourceField == "session_custom_dimensions" {
			return schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_dimensions"), sessionSourceField, nil
		}
		return schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables"), sessionSourceField, nil
	default:
		return "", "", fmt.Errorf("%s has invalid value %q", path, scope)
	}
}

func defaultScope(scope properties.CustomColumnScope) properties.CustomColumnScope {
	if scope == "" {
		return properties.CustomColumnScopeEvent
	}
	return scope
}

func defaultType(columnType properties.CustomColumnType) properties.CustomColumnType {
	if columnType == "" {
		return properties.CustomColumnTypeString
	}
	return columnType
}

func validateType(path string, columnType properties.CustomColumnType) error {
	switch columnType {
	case properties.CustomColumnTypeString,
		properties.CustomColumnTypeInt64,
		properties.CustomColumnTypeFloat64,
		properties.CustomColumnTypeBool:
		return nil
	default:
		return fmt.Errorf("%s has invalid value %q", path, columnType)
	}
}
