package customcolumns

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPropertyCustomColumnsRegistryGet_NoCustomColumnsReturnsEmpty(t *testing.T) {
	// given
	psr := properties.NewStaticSettingsRegistry([]properties.Settings{}, properties.WithDefaultConfig(&properties.Settings{
		PropertyID: "property-1",
	}))
	r := NewPropertyCustomColumnsRegistry(psr, nil)

	// when
	cols, err := r.Get("property-1")

	// then
	require.NoError(t, err)
	assert.Empty(t, cols.Event)
	assert.Empty(t, cols.Session)
	assert.Empty(t, cols.SessionScopedEvent)
}

func TestPropertyCustomColumnsRegistryGet_PropertyLookupErrorIncludesContext(t *testing.T) {
	// given
	psr := properties.NewStaticSettingsRegistry([]properties.Settings{})
	r := NewPropertyCustomColumnsRegistry(psr, nil)

	// when
	_, err := r.Get("missing-property")

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get settings for property \"missing-property\"")
}

func TestPropertyCustomColumnsRegistryGet_BuildErrorIncludesContext(t *testing.T) {
	// given
	psr := properties.NewStaticSettingsRegistry([]properties.Settings{}, properties.WithDefaultConfig(&properties.Settings{
		PropertyID: "property-1",
		CustomColumns: []properties.CustomColumnConfig{{
			Name: "broken",
		}},
	}))
	r := NewPropertyCustomColumnsRegistry(psr, nil)

	// when
	_, err := r.Get("property-1")

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "build custom columns for property \"property-1\"")
}

func TestPropertyCustomColumnsRegistryGet_BuildsConfiguredColumns(t *testing.T) {
	// given
	psr := properties.NewStaticSettingsRegistry([]properties.Settings{}, properties.WithDefaultConfig(&properties.Settings{
		PropertyID: "property-1",
		CustomColumns: []properties.CustomColumnConfig{
			{
				Name:  "ga_param",
				Scope: properties.CustomColumnScopeEvent,
				Type:  properties.CustomColumnTypeString,
				DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID(
					"ga4.protocols.d8a.tech/event/params",
				)},
				Implementation: properties.NestedLookupConfig{
					SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
					SourceField:       "params",
					MatchField:        "name",
					MatchEquals:       "ga_param",
					ValueField:        "value_string",
				},
			},
			{
				Name:  "m_var",
				Scope: properties.CustomColumnScopeSession,
				Type:  properties.CustomColumnTypeString,
				DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID(
					"matomo.protocols.d8a.tech/session/session_custom_variables",
				)},
				Implementation: properties.NestedLookupConfig{
					SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables"),
					SourceField:       "session_custom_variables",
					MatchField:        "name",
					MatchEquals:       "m_var",
					ValueField:        "value",
					Pick:              properties.NestedLookupPickStrategyLastNonNull,
				},
			},
		},
	}))
	r := NewPropertyCustomColumnsRegistry(psr, NewRegistry(nil))

	// when
	cols, err := r.Get("property-1")

	// then
	require.NoError(t, err)
	assert.Len(t, cols.Event, 1)
	assert.Len(t, cols.Session, 1)
	assert.Equal(t, schema.InterfaceID("customcolumns.d8a.tech/event/ga_param"), cols.Event[0].Implements().ID)
	assert.Equal(t, schema.InterfaceID("customcolumns.d8a.tech/session/m_var"), cols.Session[0].Implements().ID)
}
