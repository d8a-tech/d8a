package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseProtocolCustomColumnsConfig(t *testing.T) {
	// given
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(configPath, []byte(`
ga4:
  params:
    - name: ga_param
matomo:
  custom_dimensions:
    - slot: 3
      name: m_dim
  custom_variables:
    - name: m_var
`), 0o600)
	require.NoError(t, err)

	// when
	parsed, err := parseProtocolCustomColumnsConfig(configPath)

	// then
	require.NoError(t, err)
	assert.Len(t, parsed.GA4.Params, 1)
	assert.Equal(t, "ga_param", parsed.GA4.Params[0].Name)
	assert.Len(t, parsed.Matomo.CustomDimensions, 1)
	assert.Equal(t, int64(3), parsed.Matomo.CustomDimensions[0].Slot)
	assert.Equal(t, "m_dim", parsed.Matomo.CustomDimensions[0].Name)
	assert.Len(t, parsed.Matomo.CustomVariables, 1)
	assert.Equal(t, "m_var", parsed.Matomo.CustomVariables[0].Name)
}

func TestShortcutCustomColumnNormalizer_Normalize(t *testing.T) {
	// given
	normalizer := newProtocolCustomColumnNormalizer(newProtocolCustomColumnValidator())

	// when
	columns, err := normalizer.Normalize(protocolCustomColumnsConfig{
		GA4: ga4CustomColumnsConfig{Params: []ga4ParamShortcutConfig{{
			Name: "ga_param",
		}}},
		Matomo: matomoCustomColumnsConfig{
			CustomDimensions: []matomoCustomDimensionShortcutConfig{{
				Slot: 2,
				Name: "m_dim_event",
			}, {
				Slot:  8,
				Name:  "m_dim_session",
				Scope: properties.CustomColumnScopeSession,
			}},
			CustomVariables: []matomoCustomVariableShortcutConfig{{
				Name: "m_var_event",
			}, {
				Name:  "m_var_session",
				Scope: properties.CustomColumnScopeSession,
			}},
		},
	})

	// then
	require.NoError(t, err)
	require.Len(t, columns, 5)

	assert.Equal(t, properties.CustomColumnConfig{
		Name:      "ga_param",
		Scope:     properties.CustomColumnScopeEvent,
		Type:      properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("ga4.protocols.d8a.tech/event/params")},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
			SourceField:       "params",
			MatchField:        "name",
			MatchEquals:       "ga_param",
			ValueField:        "value_string",
		},
	}, columns[0])

	assert.Equal(t, properties.CustomColumnConfig{
		Name:      "m_dim_event",
		Scope:     properties.CustomColumnScopeEvent,
		Type:      properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_dimensions")},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_dimensions"),
			SourceField:       "custom_dimensions",
			MatchField:        "slot",
			MatchEquals:       int64(2),
			ValueField:        "value",
		},
	}, columns[1])

	assert.Equal(t, properties.CustomColumnConfig{
		Name:  "m_dim_session",
		Scope: properties.CustomColumnScopeSession,
		Type:  properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{
			Interface: schema.InterfaceID(
				"matomo.protocols.d8a.tech/session/session_custom_dimensions",
			),
		},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeSession,
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_dimensions"),
			SourceField:       "session_custom_dimensions",
			MatchField:        "slot",
			MatchEquals:       int64(8),
			ValueField:        "value",
			Pick:              properties.NestedLookupPickStrategyLastNonNull,
		},
	}, columns[2])

	assert.Equal(t, properties.CustomColumnConfig{
		Name:      "m_var_event",
		Scope:     properties.CustomColumnScopeEvent,
		Type:      properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_variables")},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_variables"),
			SourceField:       "custom_variables",
			MatchField:        "name",
			MatchEquals:       "m_var_event",
			ValueField:        "value",
		},
	}, columns[3])

	assert.Equal(t, properties.CustomColumnConfig{
		Name:  "m_var_session",
		Scope: properties.CustomColumnScopeSession,
		Type:  properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{
			Interface: schema.InterfaceID(
				"matomo.protocols.d8a.tech/session/session_custom_variables",
			),
		},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeSession,
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables"),
			SourceField:       "session_custom_variables",
			MatchField:        "name",
			MatchEquals:       "m_var_session",
			ValueField:        "value",
			Pick:              properties.NestedLookupPickStrategyLastNonNull,
		},
	}, columns[4])
}

func TestShortcutCustomColumnNormalizer_NormalizeValidation(t *testing.T) {
	tests := []struct {
		name      string
		shortcuts protocolCustomColumnsConfig
		errPart   string
	}{
		{
			name: "missing ga4 name",
			shortcuts: protocolCustomColumnsConfig{
				GA4: ga4CustomColumnsConfig{Params: []ga4ParamShortcutConfig{{}}},
			},
			errPart: "ga4.params[0].name is required",
		},
		{
			name: "invalid ga4 scope",
			shortcuts: protocolCustomColumnsConfig{
				GA4: ga4CustomColumnsConfig{Params: []ga4ParamShortcutConfig{{
					Name:  "x",
					Scope: properties.CustomColumnScopeSession,
				}}},
			},
			errPart: "ga4.params[0].scope must be \"event\"",
		},
		{
			name: "invalid type",
			shortcuts: protocolCustomColumnsConfig{
				Matomo: matomoCustomColumnsConfig{CustomVariables: []matomoCustomVariableShortcutConfig{{
					Name: "x",
					Type: properties.CustomColumnType("unknown"),
				}}},
			},
			errPart: "matomo.custom_variables[0].type has invalid value \"unknown\"",
		},
		{
			name: "ga4 bool rejected",
			shortcuts: protocolCustomColumnsConfig{
				GA4: ga4CustomColumnsConfig{Params: []ga4ParamShortcutConfig{{
					Name: "x",
					Type: properties.CustomColumnTypeBool,
				}}},
			},
			errPart: "ga4.params[0].type \"bool\" is unsupported",
		},
		{
			name: "missing matomo dimension slot",
			shortcuts: protocolCustomColumnsConfig{
				Matomo: matomoCustomColumnsConfig{CustomDimensions: []matomoCustomDimensionShortcutConfig{{
					Name: "x",
				}}},
			},
			errPart: "matomo.custom_dimensions[0].slot is required",
		},
		{
			name: "invalid matomo dimension scope",
			shortcuts: protocolCustomColumnsConfig{
				Matomo: matomoCustomColumnsConfig{CustomDimensions: []matomoCustomDimensionShortcutConfig{{
					Name:  "x",
					Slot:  1,
					Scope: properties.CustomColumnScopeSessionScopedEvent,
				}}},
			},
			errPart: "matomo.custom_dimensions[0].scope has invalid value \"session_scoped_event\"",
		},
		{
			name: "duplicate name across groups",
			shortcuts: protocolCustomColumnsConfig{
				GA4:    ga4CustomColumnsConfig{Params: []ga4ParamShortcutConfig{{Name: "shared"}}},
				Matomo: matomoCustomColumnsConfig{CustomVariables: []matomoCustomVariableShortcutConfig{{Name: "shared"}}},
			},
			errPart: "duplicate custom column output name \"shared\"",
		},
		{
			name: "matomo dimension int64 rejected",
			shortcuts: protocolCustomColumnsConfig{
				Matomo: matomoCustomColumnsConfig{CustomDimensions: []matomoCustomDimensionShortcutConfig{{
					Name: "x",
					Slot: 1,
					Type: properties.CustomColumnTypeInt64,
				}}},
			},
			errPart: "matomo.custom_dimensions[0].type \"int64\" is unsupported",
		},
		{
			name: "matomo dimension float64 rejected",
			shortcuts: protocolCustomColumnsConfig{
				Matomo: matomoCustomColumnsConfig{CustomDimensions: []matomoCustomDimensionShortcutConfig{{
					Name: "x",
					Slot: 1,
					Type: properties.CustomColumnTypeFloat64,
				}}},
			},
			errPart: "matomo.custom_dimensions[0].type \"float64\" is unsupported",
		},
		{
			name: "matomo dimension bool rejected",
			shortcuts: protocolCustomColumnsConfig{
				Matomo: matomoCustomColumnsConfig{CustomDimensions: []matomoCustomDimensionShortcutConfig{{
					Name: "x",
					Slot: 1,
					Type: properties.CustomColumnTypeBool,
				}}},
			},
			errPart: "matomo.custom_dimensions[0].type \"bool\" is unsupported",
		},
		{
			name: "matomo variable int64 rejected",
			shortcuts: protocolCustomColumnsConfig{
				Matomo: matomoCustomColumnsConfig{CustomVariables: []matomoCustomVariableShortcutConfig{{
					Name: "x",
					Type: properties.CustomColumnTypeInt64,
				}}},
			},
			errPart: "matomo.custom_variables[0].type \"int64\" is unsupported",
		},
		{
			name: "matomo variable float64 rejected",
			shortcuts: protocolCustomColumnsConfig{
				Matomo: matomoCustomColumnsConfig{CustomVariables: []matomoCustomVariableShortcutConfig{{
					Name: "x",
					Type: properties.CustomColumnTypeFloat64,
				}}},
			},
			errPart: "matomo.custom_variables[0].type \"float64\" is unsupported",
		},
		{
			name: "matomo variable bool rejected",
			shortcuts: protocolCustomColumnsConfig{
				Matomo: matomoCustomColumnsConfig{CustomVariables: []matomoCustomVariableShortcutConfig{{
					Name: "x",
					Type: properties.CustomColumnTypeBool,
				}}},
			},
			errPart: "matomo.custom_variables[0].type \"bool\" is unsupported",
		},
	}

	normalizer := newProtocolCustomColumnNormalizer(newProtocolCustomColumnValidator())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			_, err := normalizer.Normalize(tt.shortcuts)

			// then
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errPart)
		})
	}
}

func TestLoadProtocolCustomColumns_AppendsFlagJSONToYAML(t *testing.T) {
	// given
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(configPath, []byte(`
ga4:
  params:
    - name: from_yaml
`), 0o600)
	require.NoError(t, err)

	oldConfigFile := configFile
	configFile = configPath
	t.Cleanup(func() {
		configFile = oldConfigFile
	})

	command := staticProtocolCustomColumnsSource{values: map[string][]string{
		ga4ParamsFlag.Name: {`{"name":"from_flag"}`},
	}}

	// when
	columns, err := loadProtocolCustomColumns(command)

	// then
	require.NoError(t, err)
	assert.Len(t, columns, 2)
	assert.Equal(t, "from_yaml", columns[0].Name)
	assert.Equal(t, "from_flag", columns[1].Name)
}

type staticProtocolCustomColumnsSource struct {
	values map[string][]string
}

func (s staticProtocolCustomColumnsSource) StringSlice(name string) []string {
	return s.values[name]
}
