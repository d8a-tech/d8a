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
	f := NewMultiColumnBuilder(NewEventColumnBuilder(), NewSessionColumnBuilder())
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
	built, err := buildOne(t, f, &def)
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
	f := NewMultiColumnBuilder(NewEventColumnBuilder(), NewSessionColumnBuilder())
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
	built, err := buildOne(t, f, &def)
	require.NoError(t, err)
	writeErr := built.Event.Write(event)

	// then
	require.Error(t, writeErr)
	assert.Contains(t, writeErr.Error(), "read repeated records")
}

func TestFactoryBuild_SessionColumnSuccess(t *testing.T) {
	// given
	f := NewMultiColumnBuilder(NewEventColumnBuilder(), NewSessionColumnBuilder())
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
	built, err := buildOne(t, f, &def)
	require.NoError(t, err)
	writeErr := built.Session.Write(session)

	// then
	require.NoError(t, writeErr)
	assert.Equal(t, "beta", session.Values["matomo_dimension_3"])
}

func TestFactoryBuild_SessionColumnInvalidSource(t *testing.T) {
	// given
	f := NewMultiColumnBuilder(NewEventColumnBuilder(), NewSessionColumnBuilder())
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
	built, err := buildOne(t, f, &def)
	require.NoError(t, err)
	writeErr := built.Session.Write(session)

	// then
	require.Error(t, writeErr)
	assert.Contains(t, writeErr.Error(), "read repeated records")
}

func TestBuilderBuild_GroupsColumnsByScope(t *testing.T) {
	// given
	r := NewBuilder()
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
	}

	defPtrs := make([]*properties.CustomColumnConfig, 0, len(defs))
	for i := range defs {
		defPtrs = append(defPtrs, &defs[i])
	}

	// when
	built, err := r.Build(defPtrs)

	// then
	require.NoError(t, err)

	cols := schema.Columns{}
	for i := range built {
		if built[i].Event != nil {
			cols.Event = append(cols.Event, built[i].Event)
		}
		if built[i].Session != nil {
			cols.Session = append(cols.Session, built[i].Session)
		}
		if built[i].SessionScopedEvent != nil {
			cols.SessionScopedEvent = append(cols.SessionScopedEvent, built[i].SessionScopedEvent)
		}
	}

	assert.Len(t, cols.Event, 1)
	assert.Len(t, cols.Session, 1)
	assert.Empty(t, cols.SessionScopedEvent)
}

func TestFactoryBuild_StartupValidationFailures(t *testing.T) {
	// given
	f := NewMultiColumnBuilder(NewEventColumnBuilder(), NewSessionColumnBuilder())
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
			name: "unsupported source scope for session",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Scope = properties.CustomColumnScopeSession
				def.Implementation.SourceScope = properties.NestedLookupSourceScope("profile")
			},
			errSubstr: "unsupported custom column implementation.source_scope",
		},
		{
			name: "unsupported pick strategy",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Scope = properties.CustomColumnScopeSession
				def.Implementation.Pick = properties.NestedLookupPickStrategy("first")
			},
			errSubstr: "unsupported pick strategy",
		},
		{
			name: "event scope cannot use pick",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Implementation.Pick = properties.NestedLookupPickStrategyLastNonNull
			},
			errSubstr: "event custom columns cannot use implementation.pick",
		},
		{
			name: "session scoped event is unsupported",
			mutate: func(def *properties.CustomColumnConfig) {
				def.Scope = properties.CustomColumnScopeSessionScopedEvent
			},
			errSubstr: "is not supported by nested lookup custom columns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := base
			tt.mutate(&def)

			// when
			_, err := f.Build([]*properties.CustomColumnConfig{&def})

			// then
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errSubstr)
		})
	}
}

func TestFactoryBuild_SessionScopedEventIsUnsupported(t *testing.T) {
	// given
	f := NewMultiColumnBuilder(NewEventColumnBuilder(), NewSessionColumnBuilder())
	def := properties.CustomColumnConfig{
		Name:      "session_plan_active",
		Scope:     properties.CustomColumnScopeSessionScopedEvent,
		Type:      properties.CustomColumnTypeString,
		DependsOn: schema.DependsOnEntry{Interface: schema.InterfaceID("ga4.protocols.d8a.tech/event/params")},
		Implementation: properties.NestedLookupConfig{
			SourceScope:       properties.NestedLookupSourceScopeEvent,
			SourceInterfaceID: schema.InterfaceID("ga4.protocols.d8a.tech/event/params"),
			SourceField:       "params",
			MatchField:        "name",
			MatchEquals:       "plan_active",
			ValueField:        "value_string",
			Pick:              properties.NestedLookupPickStrategyLastNonNull,
		},
	}
	// when
	_, err := f.Build([]*properties.CustomColumnConfig{&def})

	// then
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not supported by nested lookup custom columns")
}

func buildOne(t *testing.T, b ColumnBuilder, def *properties.CustomColumnConfig) (BuiltColumn, error) {
	t.Helper()

	built, err := b.Build([]*properties.CustomColumnConfig{def})
	if err != nil {
		return BuiltColumn{}, err
	}
	require.Len(t, built, 1)

	return built[0], nil
}
