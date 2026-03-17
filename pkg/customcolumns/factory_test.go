package customcolumns

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validEventDefinition() properties.CustomColumnConfig {
	return properties.CustomColumnConfig{
		Name:  "ga_score",
		Scope: properties.CustomColumnScopeEvent,
		Type:  properties.CustomColumnTypeInt64,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID(
			"ga4.protocols.d8a.tech/event/params",
		)},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
			SourceField:       "params",
			MatchField:        "name",
			MatchEquals:       "score",
			ValueField:        "value_number",
		},
	}
}

func TestFactoryBuild_EventColumnSuccess(t *testing.T) {
	// given
	f := NewFactory()
	def := validEventDefinition()
	event := &schema.Event{Values: map[string]any{
		"params": []any{
			"bad-row",
			map[string]any{"name": "score", "value_number": nil},
			map[string]any{"name": "score", "value_number": 42.0},
			map[string]any{"name": "other", "value_number": 11.0},
		},
	}}

	// when
	built, err := f.Build(&def)
	require.NoError(t, err)
	writeErr := built.Event.Write(event)

	// then
	require.NoError(t, writeErr)
	assert.Equal(t, int64(42), event.Values["ga_score"])
	assert.Equal(t, []schema.DependsOnEntry{def.DependsOn}, built.Event.DependsOn())
	assert.Equal(t, "customcolumns.d8a.tech/event/ga_score", string(built.Event.Implements().ID))
}

func TestFactoryBuild_EventColumnInvalidSource(t *testing.T) {
	// given
	f := NewFactory()
	def := properties.CustomColumnConfig{
		Name:      "ga_invalid",
		Scope:     properties.CustomColumnScopeEvent,
		Type:      properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("ga4.protocols.d8a.tech/event/params")},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
			SourceField:       "params",
			MatchField:        "name",
			MatchEquals:       "x",
			ValueField:        "value_string",
		},
	}
	event := &schema.Event{Values: map[string]any{"params": "not-repeated"}}

	// when
	built, err := f.Build(&def)
	require.NoError(t, err)
	writeErr := built.Event.Write(event)

	// then
	require.Error(t, writeErr)
	assert.Contains(t, writeErr.Error(), "read repeated records")
}

func TestFactoryBuild_SessionColumnSuccess(t *testing.T) {
	// given
	f := NewFactory()
	def := properties.CustomColumnConfig{
		Name:  "matomo_dimension_3",
		Scope: properties.CustomColumnScopeSession,
		Type:  properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID(
			"matomo.protocols.d8a.tech/event/custom_dimensions",
		)},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_dimensions"),
			SourceField:       "custom_dimensions",
			MatchField:        "slot",
			MatchEquals:       int64(3),
			ValueField:        "value",
			Pick:              properties.NestedLookupPickStrategyLastNonNull,
		},
	}
	session := &schema.Session{
		Values: map[string]any{},
		Events: []*schema.Event{
			{Values: map[string]any{"custom_dimensions": []any{map[string]any{"slot": int64(3), "value": "alpha"}}}},
			{Values: map[string]any{"custom_dimensions": []any{
				map[string]any{"slot": int64(3)},
				map[string]any{"slot": int64(3), "value": "beta"},
			}}},
		},
	}

	// when
	built, err := f.Build(&def)
	require.NoError(t, err)
	writeErr := built.Session.Write(session)

	// then
	require.NoError(t, writeErr)
	assert.Equal(t, "beta", session.Values["matomo_dimension_3"])
}

func TestFactoryBuild_SessionColumnInvalidSource(t *testing.T) {
	// given
	f := NewFactory()
	def := properties.CustomColumnConfig{
		Name:      "matomo_invalid",
		Scope:     properties.CustomColumnScopeSession,
		Type:      properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_dimensions")},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/event/custom_dimensions"),
			SourceField:       "custom_dimensions",
			MatchField:        "slot",
			MatchEquals:       int64(1),
			ValueField:        "value",
			Pick:              properties.NestedLookupPickStrategyLastNonNull,
		},
	}
	session := &schema.Session{
		Values: map[string]any{},
		Events: []*schema.Event{{
			Values: map[string]any{
				"custom_dimensions": map[string]any{"slot": int64(1), "value": "nope"},
			},
		}},
	}

	// when
	built, err := f.Build(&def)
	require.NoError(t, err)
	writeErr := built.Session.Write(session)

	// then
	require.Error(t, writeErr)
	assert.Contains(t, writeErr.Error(), "read repeated records")
}

func TestFactoryBuild_SessionScopedEventColumnSuccess(t *testing.T) {
	// given
	f := NewFactory()
	def := properties.CustomColumnConfig{
		Name:  "session_plan_active",
		Scope: properties.CustomColumnScopeSessionScopedEvent,
		Type:  properties.CustomColumnTypeBool,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID(
			"matomo.protocols.d8a.tech/session/session_custom_variables",
		)},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeSession,
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables"),
			SourceField:       "session_custom_variables",
			MatchField:        "name",
			MatchEquals:       "plan_active",
			ValueField:        "value",
			Pick:              properties.NestedLookupPickStrategyLastNonNull,
		},
	}
	session := &schema.Session{
		Values: map[string]any{
			"session_custom_variables": []any{
				map[string]any{"name": "plan_active"},
				map[string]any{"name": "plan_active", "value": "true"},
			},
		},
		Events: []*schema.Event{{Values: map[string]any{}}, {Values: map[string]any{}}},
	}

	// when
	built, err := f.Build(&def)
	require.NoError(t, err)
	writeErr0 := built.SessionScopedEvent.Write(session, 0)
	writeErr1 := built.SessionScopedEvent.Write(session, 1)

	// then
	require.NoError(t, writeErr0)
	require.NoError(t, writeErr1)
	assert.Equal(t, true, session.Events[0].Values["session_plan_active"])
	assert.Equal(t, true, session.Events[1].Values["session_plan_active"])
}

func TestFactoryBuild_SessionScopedEventColumnInvalidSource(t *testing.T) {
	// given
	f := NewFactory()
	def := properties.CustomColumnConfig{
		Name:      "event_bad",
		Scope:     properties.CustomColumnScopeSessionScopedEvent,
		Type:      properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("ga4.protocols.d8a.tech/event/params")},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
			SourceField:       "params",
			MatchField:        "name",
			MatchEquals:       "x",
			ValueField:        "value_string",
		},
	}
	session := &schema.Session{
		Values: map[string]any{},
		Events: []*schema.Event{{Values: map[string]any{"params": 123}}},
	}

	// when
	built, err := f.Build(&def)
	require.NoError(t, err)
	writeErr := built.SessionScopedEvent.Write(session, 0)

	// then
	require.Error(t, writeErr)
	assert.Contains(t, writeErr.Error(), "read repeated records")
}

func TestRegistryBuildAll_GroupsColumnsByScope(t *testing.T) {
	// given
	r := NewRegistry(nil)
	defs := []properties.CustomColumnConfig{
		{
			Name:      "ev",
			Scope:     properties.CustomColumnScopeEvent,
			Type:      properties.CustomColumnTypeString,
			DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("ga4.protocols.d8a.tech/event/params")},
			Implementation: properties.NestedLookupConfig{
				SourceScope:       properties.NestedLookupSourceScopeEvent,
				SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
				SourceField:       "params",
				MatchField:        "name",
				MatchEquals:       "ev",
				ValueField:        "value_string",
			},
		},
		{
			Name:  "sess",
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
				MatchEquals:       "sess",
				ValueField:        "value",
				Pick:              properties.NestedLookupPickStrategyLastNonNull,
			},
		},
		{
			Name:      "sse",
			Scope:     properties.CustomColumnScopeSessionScopedEvent,
			Type:      properties.CustomColumnTypeString,
			DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("ga4.protocols.d8a.tech/event/params")},
			Implementation: properties.NestedLookupConfig{
				SourceScope:       properties.NestedLookupSourceScopeEvent,
				SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
				SourceField:       "params",
				MatchField:        "name",
				MatchEquals:       "sse",
				ValueField:        "value_string",
			},
		},
	}

	// when
	cols, err := r.BuildAll(defs)

	// then
	require.NoError(t, err)
	assert.Len(t, cols.Event, 1)
	assert.Len(t, cols.Session, 1)
	assert.Len(t, cols.SessionScopedEvent, 1)
}

func TestFactoryBuild_StartupValidationFailures(t *testing.T) {
	// given
	f := NewFactory()
	base := validEventDefinition()

	tests := []struct {
		name      string
		mutate    func(*properties.CustomColumnConfig)
		errSubstr string
	}{
		{
			name: "empty scope",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Scope = ""
			},
			errSubstr: "scope is required",
		},
		{
			name: "unsupported scope",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Scope = properties.CustomColumnScope("profile")
			},
			errSubstr: "unsupported custom column scope",
		},
		{
			name: "empty type",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Type = ""
			},
			errSubstr: "type is required",
		},
		{
			name: "missing depends_on interface",
			mutate: func(def *properties.CustomColumnConfig) {
				def.DependsOn.Interface = ""
				def.Implementation.SourceInterfaceID = ""
			},
			errSubstr: "depends_on.interface is required",
		},
		{
			name: "empty source_field",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Implementation.SourceField = ""
			},
			errSubstr: "source_field is required",
		},
		{
			name: "empty match_field",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Implementation.MatchField = ""
			},
			errSubstr: "match_field is required",
		},
		{
			name: "empty value_field",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Implementation.ValueField = ""
			},
			errSubstr: "value_field is required",
		},
		{
			name: "unsupported source interface",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Scope = properties.CustomColumnScopeSessionScopedEvent
				def.Implementation.SourceScope = ""
				def.Implementation.SourceInterfaceID = "unknown.protocols.d8a.tech/source"
			},
			errSubstr: "cannot infer source scope from implementation.source_interface_id",
		},
		{
			name: "unsupported pick strategy",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Implementation.Pick = properties.NestedLookupPickStrategy("first")
			},
			errSubstr: "unsupported pick strategy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := base
			tt.mutate(&def)

			// when
			_, err := f.Build(&def)

			// then
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errSubstr)
		})
	}
}

func TestFactoryBuild_UsesSourceInterfaceOverDependsOnNaming(t *testing.T) {
	// given
	f := NewFactory()
	def := properties.CustomColumnConfig{
		Name:  "session_plan_active",
		Scope: properties.CustomColumnScopeSessionScopedEvent,
		Type:  properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID(
			"matomo.protocols.d8a.tech/event/custom_variables",
		)},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeSession,
			SourceInterfaceID: schema.InterfaceID("matomo.protocols.d8a.tech/session/session_custom_variables"),
			SourceField:       "session_custom_variables",
			MatchField:        "name",
			MatchEquals:       "plan_active",
			ValueField:        "value",
			Pick:              properties.NestedLookupPickStrategyLastNonNull,
		},
	}
	session := &schema.Session{
		Values: map[string]any{
			"session_custom_variables": []any{map[string]any{"name": "plan_active", "value": "gold"}},
		},
		Events: []*schema.Event{{Values: map[string]any{"session_custom_variables": []any{}}}},
	}

	// when
	built, err := f.Build(&def)
	require.NoError(t, err)
	writeErr := built.SessionScopedEvent.Write(session, 0)

	// then
	require.NoError(t, writeErr)
	assert.Equal(t, "gold", session.Events[0].Values["session_plan_active"])
}
