package properties

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseProtocolCustomColumnsConfig(t *testing.T) {
	// given
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(configPath, []byte(`
protocol:
  ga4_params:
    - name: ga_param
  matomo_custom_dimensions:
    - slot: 3
      name: m_dim
  matomo_custom_variables:
    - name: m_var
`), 0o600)
	require.NoError(t, err)

	// when
	parsed, err := ParseProtocolCustomColumnsConfig(configPath)

	// then
	require.NoError(t, err)
	assert.Len(t, parsed.GA4Params, 1)
	assert.Equal(t, "ga_param", parsed.GA4Params[0].Name)
	assert.Len(t, parsed.MatomoCustomDimensions, 1)
	assert.Equal(t, int64(3), parsed.MatomoCustomDimensions[0].Slot)
	assert.Equal(t, "m_dim", parsed.MatomoCustomDimensions[0].Name)
	assert.Len(t, parsed.MatomoCustomVariables, 1)
	assert.Equal(t, "m_var", parsed.MatomoCustomVariables[0].Name)
}

func TestCustomColumnNormalizer_Normalize(t *testing.T) {
	// given
	normalizer := NewCustomColumnNormalizer(NewCustomColumnValidator())

	// when
	columns, err := normalizer.Normalize(ProtocolCustomColumnsConfig{
		GA4Params: []GA4ParamShortcutConfig{{
			Name: "ga_param",
		}},
		MatomoCustomDimensions: []MatomoCustomDimensionShortcutConfig{{
			Slot: 2,
			Name: "m_dim_event",
		}, {
			Slot:  8,
			Name:  "m_dim_session",
			Scope: CustomColumnScopeSession,
		}},
		MatomoCustomVariables: []MatomoCustomVariableShortcutConfig{{
			Name: "m_var_event",
		}, {
			Name:  "m_var_session",
			Scope: CustomColumnScopeSession,
		}},
	})

	// then
	require.NoError(t, err)
	require.Len(t, columns, 5)

	assert.Equal(t, CustomColumnConfig{
		Name:      "ga_param",
		Scope:     CustomColumnScopeEvent,
		Type:      CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("ga4.protocols.d8a.tech/event/params")},
		Implementation: NestedLookupConfig{
			SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
			SourceField:       "params",
			MatchField:        "name",
			MatchEquals:       "ga_param",
			ValueField:        "value_string",
		},
	}, columns[0])

	assert.Equal(t, CustomColumnConfig{
		Name:      "m_dim_event",
		Scope:     CustomColumnScopeEvent,
		Type:      CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_dimensions")},
		Implementation: NestedLookupConfig{
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_dimensions"),
			SourceField:       "custom_dimensions",
			MatchField:        "slot",
			MatchEquals:       int64(2),
			ValueField:        "value",
		},
	}, columns[1])

	assert.Equal(t, CustomColumnConfig{
		Name:      "m_dim_session",
		Scope:     CustomColumnScopeSession,
		Type:      CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_dimensions")},
		Implementation: NestedLookupConfig{
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_dimensions"),
			SourceField:       "session_custom_dimensions",
			MatchField:        "slot",
			MatchEquals:       int64(8),
			ValueField:        "value",
			Pick:              NestedLookupPickStrategyLastNonNull,
		},
	}, columns[2])

	assert.Equal(t, CustomColumnConfig{
		Name:      "m_var_event",
		Scope:     CustomColumnScopeEvent,
		Type:      CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_variables")},
		Implementation: NestedLookupConfig{
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_variables"),
			SourceField:       "custom_variables",
			MatchField:        "name",
			MatchEquals:       "m_var_event",
			ValueField:        "value",
		},
	}, columns[3])

	assert.Equal(t, CustomColumnConfig{
		Name:      "m_var_session",
		Scope:     CustomColumnScopeSession,
		Type:      CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables")},
		Implementation: NestedLookupConfig{
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables"),
			SourceField:       "session_custom_variables",
			MatchField:        "name",
			MatchEquals:       "m_var_session",
			ValueField:        "value",
			Pick:              NestedLookupPickStrategyLastNonNull,
		},
	}, columns[4])
}

func TestCustomColumnNormalizer_NormalizeValidation(t *testing.T) {
	tests := []struct {
		name      string
		shortcuts ProtocolCustomColumnsConfig
		errPart   string
	}{
		{
			name: "missing ga4 name",
			shortcuts: ProtocolCustomColumnsConfig{
				GA4Params: []GA4ParamShortcutConfig{{}},
			},
			errPart: "protocol.ga4_params[0].name is required",
		},
		{
			name: "invalid ga4 scope",
			shortcuts: ProtocolCustomColumnsConfig{
				GA4Params: []GA4ParamShortcutConfig{{
					Name:  "x",
					Scope: CustomColumnScopeSession,
				}},
			},
			errPart: "protocol.ga4_params[0].scope must be \"event\"",
		},
		{
			name: "invalid type",
			shortcuts: ProtocolCustomColumnsConfig{
				MatomoCustomVariables: []MatomoCustomVariableShortcutConfig{{
					Name: "x",
					Type: CustomColumnType("unknown"),
				}},
			},
			errPart: "protocol.matomo_custom_variables[0].type has invalid value \"unknown\"",
		},
		{
			name: "ga4 bool rejected",
			shortcuts: ProtocolCustomColumnsConfig{
				GA4Params: []GA4ParamShortcutConfig{{
					Name: "x",
					Type: CustomColumnTypeBool,
				}},
			},
			errPart: "protocol.ga4_params[0].type \"bool\" is unsupported",
		},
		{
			name: "missing matomo dimension slot",
			shortcuts: ProtocolCustomColumnsConfig{
				MatomoCustomDimensions: []MatomoCustomDimensionShortcutConfig{{
					Name: "x",
				}},
			},
			errPart: "protocol.matomo_custom_dimensions[0].slot is required",
		},
		{
			name: "invalid matomo dimension scope",
			shortcuts: ProtocolCustomColumnsConfig{
				MatomoCustomDimensions: []MatomoCustomDimensionShortcutConfig{{
					Name:  "x",
					Slot:  1,
					Scope: CustomColumnScopeSessionScopedEvent,
				}},
			},
			errPart: "protocol.matomo_custom_dimensions[0].scope has invalid value \"session_scoped_event\"",
		},
		{
			name: "duplicate name across groups",
			shortcuts: ProtocolCustomColumnsConfig{
				GA4Params: []GA4ParamShortcutConfig{{
					Name: "shared",
				}},
				MatomoCustomVariables: []MatomoCustomVariableShortcutConfig{{
					Name: "shared",
				}},
			},
			errPart: "duplicate custom column output name \"shared\"",
		},
		{
			name: "matomo dimension int64 rejected",
			shortcuts: ProtocolCustomColumnsConfig{
				MatomoCustomDimensions: []MatomoCustomDimensionShortcutConfig{{
					Name: "x",
					Slot: 1,
					Type: CustomColumnTypeInt64,
				}},
			},
			errPart: "protocol.matomo_custom_dimensions[0].type \"int64\" is unsupported",
		},
		{
			name: "matomo dimension float64 rejected",
			shortcuts: ProtocolCustomColumnsConfig{
				MatomoCustomDimensions: []MatomoCustomDimensionShortcutConfig{{
					Name: "x",
					Slot: 1,
					Type: CustomColumnTypeFloat64,
				}},
			},
			errPart: "protocol.matomo_custom_dimensions[0].type \"float64\" is unsupported",
		},
		{
			name: "matomo dimension bool rejected",
			shortcuts: ProtocolCustomColumnsConfig{
				MatomoCustomDimensions: []MatomoCustomDimensionShortcutConfig{{
					Name: "x",
					Slot: 1,
					Type: CustomColumnTypeBool,
				}},
			},
			errPart: "protocol.matomo_custom_dimensions[0].type \"bool\" is unsupported",
		},
		{
			name: "matomo variable int64 rejected",
			shortcuts: ProtocolCustomColumnsConfig{
				MatomoCustomVariables: []MatomoCustomVariableShortcutConfig{{
					Name: "x",
					Type: CustomColumnTypeInt64,
				}},
			},
			errPart: "protocol.matomo_custom_variables[0].type \"int64\" is unsupported",
		},
		{
			name: "matomo variable float64 rejected",
			shortcuts: ProtocolCustomColumnsConfig{
				MatomoCustomVariables: []MatomoCustomVariableShortcutConfig{{
					Name: "x",
					Type: CustomColumnTypeFloat64,
				}},
			},
			errPart: "protocol.matomo_custom_variables[0].type \"float64\" is unsupported",
		},
		{
			name: "matomo variable bool rejected",
			shortcuts: ProtocolCustomColumnsConfig{
				MatomoCustomVariables: []MatomoCustomVariableShortcutConfig{{
					Name: "x",
					Type: CustomColumnTypeBool,
				}},
			},
			errPart: "protocol.matomo_custom_variables[0].type \"bool\" is unsupported",
		},
	}

	normalizer := NewCustomColumnNormalizer(NewCustomColumnValidator())
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
