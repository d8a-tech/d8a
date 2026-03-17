package columnset

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/customcolumns"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/protocol/matomo"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultColumnRegistry_WithCustomColumnsRegistry_GA4EventColumn(t *testing.T) {
	// given
	psr := properties.NewStaticSettingsRegistry([]properties.Settings{}, properties.WithDefaultConfig(&properties.Settings{
		PropertyID:            "default",
		PropertyMeasurementID: "G-TEST",
		CustomColumns: []properties.CustomColumnConfig{{
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
		}},
	}))

	registry := DefaultColumnRegistry(
		ga4.NewGA4Protocol(currency.NewDummyConverter(1), psr),
		psr,
		WithCustomColumnsRegistry(customcolumns.NewPropertyCustomColumnsRegistry(psr, customcolumns.NewRegistry(nil))),
	)

	// when
	cols, err := registry.Get("default")

	// then
	require.NoError(t, err)
	assert.True(t, hasEventInterface(cols.Event, schema.InterfaceID("customcolumns.d8a.tech/event/ga_param")))
}

func TestDefaultColumnRegistry_WithCustomColumnsRegistry_MatomoSessionColumn(t *testing.T) {
	// given
	psr := properties.NewStaticSettingsRegistry([]properties.Settings{}, properties.WithDefaultConfig(&properties.Settings{
		PropertyID:            "default",
		PropertyMeasurementID: "1",
		CustomColumns: []properties.CustomColumnConfig{{
			Name:  "m_var_session",
			Scope: properties.CustomColumnScopeSession,
			Type:  properties.CustomColumnTypeString,
			DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID(
				"matomo.protocols.d8a.tech/session/session_custom_variables",
			)},
			Implementation: properties.NestedLookupConfig{
				SourceScope:       properties.NestedLookupSourceScopeSession,
				SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables"),
				SourceField:       "session_custom_variables",
				MatchField:        "name",
				MatchEquals:       "m_var_session",
				ValueField:        "value",
				Pick:              properties.NestedLookupPickStrategyLastNonNull,
			},
		}},
	}))

	registry := DefaultColumnRegistry(
		matomo.NewMatomoProtocol(matomo.NewFromIDSiteExtractor(psr)),
		psr,
		WithCustomColumnsRegistry(customcolumns.NewPropertyCustomColumnsRegistry(psr, customcolumns.NewRegistry(nil))),
	)

	// when
	cols, err := registry.Get("default")

	// then
	require.NoError(t, err)
	assert.True(t, hasSessionInterface(cols.Session, schema.InterfaceID("customcolumns.d8a.tech/session/m_var_session")))
}

func hasEventInterface(cols []schema.EventColumn, id schema.InterfaceID) bool {
	for _, col := range cols {
		if col.Implements().ID == id {
			return true
		}
	}

	return false
}

func hasSessionInterface(cols []schema.SessionColumn, id schema.InterfaceID) bool {
	for _, col := range cols {
		if col.Implements().ID == id {
			return true
		}
	}

	return false
}
