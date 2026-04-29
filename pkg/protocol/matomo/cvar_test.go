package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoCustomVariablesColumn(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) columntests.TestHits {
		hit := columntests.TestHitOne()
		hit.EventName = protocol.PageViewEventType
		return columntests.TestHits{hit}
	}

	type testCase struct {
		name        string
		buildHits   func(t *testing.T) columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		expected    any
		description string
	}

	testCases := []testCase{
		{
			name:      "EventCustomVariables_ValidSingle",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{columntests.EnsureQueryParam(
				0,
				"cvar",
				`{"1":["OS","iphone 5.0"]}`,
			)},
			expected: []any{
				map[string]any{"name": "OS", "value": "iphone 5.0"},
			},
			description: "Parses one custom variable from cvar payload",
		},
		{
			name:      "EventCustomVariables_ValidMultiple",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{columntests.EnsureQueryParam(
				0,
				"cvar",
				`{"1":["OS","iphone 5.0"],"2":["Locale","en::en"]}`,
			)},
			expected: []any{
				map[string]any{"name": "OS", "value": "iphone 5.0"},
				map[string]any{"name": "Locale", "value": "en::en"},
			},
			description: "Parses multiple custom variables from cvar payload",
		},
		{
			name:      "EventCustomVariables_DeterministicOrdering",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{columntests.EnsureQueryParam(
				0,
				"cvar",
				`{"10":["Ten","10"],"2":["Two","2"],"1":["One","1"],"a":["A","a"],"b":["B","b"]}`,
			)},
			expected: []any{
				map[string]any{"name": "One", "value": "1"},
				map[string]any{"name": "Two", "value": "2"},
				map[string]any{"name": "Ten", "value": "10"},
				map[string]any{"name": "A", "value": "a"},
				map[string]any{"name": "B", "value": "b"},
			},
			description: "Sorts numeric slots ascending then non-numeric keys lexicographically",
		},
		{
			name:        "EventCustomVariables_Absent",
			buildHits:   buildPageViewHit,
			expected:    []any(nil),
			description: "Returns nil when cvar parameter is absent",
		},
		{
			name:      "EventCustomVariables_MalformedPayload",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{columntests.EnsureQueryParam(
				0,
				"cvar",
				`{"1":["OS","iphone 5.0"]`,
			)},
			expected:    []any(nil),
			description: "Returns nil when cvar payload is malformed JSON",
		},
		{
			name:      "EventCustomVariables_SkipMalformedEntries",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{columntests.EnsureQueryParam(
				0,
				"cvar",
				`{"1":["OS","iphone 5.0"],"2":["OnlyName"],"3":["Too","Many","Values"]}`,
			)},
			expected: []any{
				map[string]any{"name": "OS", "value": "iphone 5.0"},
			},
			description: "Skips malformed entries while keeping valid custom variables",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				tc.buildHits(t),
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					require.NotEmpty(t, whd.WriteCalls, "expected at least one warehouse write call")
					require.NotEmpty(t, whd.WriteCalls[0].Records, "expected at least one record written")
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record["custom_variables"], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}

func TestMatomoSessionCustomVariablesColumn(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) columntests.TestHits {
		hit := columntests.TestHitOne()
		hit.EventName = protocol.PageViewEventType
		return columntests.TestHits{hit}
	}

	type testCase struct {
		name        string
		buildHits   func(t *testing.T) columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		expected    any
		description string
	}

	testCases := []testCase{
		{
			name:      "SessionCustomVariables_OneEventWithUnderscoreCvar",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{columntests.EnsureQueryParam(
				0,
				"_cvar",
				`{"1":["OS","iphone 5.0"]}`,
			)},
			expected: []any{
				map[string]any{"name": "OS", "value": "iphone 5.0"},
			},
			description: "Parses session custom variables from one event",
		},
		{
			name: "SessionCustomVariables_AccumulatesAcrossMultipleEvents",
			buildHits: func(t *testing.T) columntests.TestHits {
				h1 := columntests.TestHitOne()
				h1.EventName = protocol.PageViewEventType
				h2 := columntests.TestHitOne()
				h2.EventName = protocol.PageViewEventType
				return columntests.TestHits{h1, h2}
			},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "_cvar", `{"1":["OS","iphone"]}`),
				columntests.EnsureQueryParam(1, "_cvar", `{"1":["Locale","en"]}`),
				columntests.EnsureQueryParam(1, "v", "2"),
			},
			expected: []any{
				map[string]any{"name": "Locale", "value": "en"},
				map[string]any{"name": "OS", "value": "iphone"},
			},
			description: "Accumulates names from all events and returns deterministic order",
		},
		{
			name: "SessionCustomVariables_LaterEventOverridesEarlierValue",
			buildHits: func(t *testing.T) columntests.TestHits {
				h1 := columntests.TestHitOne()
				h1.EventName = protocol.PageViewEventType
				h2 := columntests.TestHitOne()
				h2.EventName = protocol.PageViewEventType
				return columntests.TestHits{h1, h2}
			},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "_cvar", `{"1":["OS","iphone"]}`),
				columntests.EnsureQueryParam(1, "_cvar", `{"1":["OS","android"]}`),
				columntests.EnsureQueryParam(1, "v", "2"),
			},
			expected: []any{
				map[string]any{"name": "OS", "value": "android"},
			},
			description: "Uses later event value when variable names conflict",
		},
		{
			name: "SessionCustomVariables_IndependentNamesArePreserved",
			buildHits: func(t *testing.T) columntests.TestHits {
				h1 := columntests.TestHitOne()
				h1.EventName = protocol.PageViewEventType
				h2 := columntests.TestHitOne()
				h2.EventName = protocol.PageViewEventType
				return columntests.TestHits{h1, h2}
			},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "_cvar", `{"1":["Browser","Safari"]}`),
				columntests.EnsureQueryParam(1, "_cvar", `{"2":["Country","US"]}`),
				columntests.EnsureQueryParam(1, "v", "2"),
			},
			expected: []any{
				map[string]any{"name": "Browser", "value": "Safari"},
				map[string]any{"name": "Country", "value": "US"},
			},
			description: "Keeps independent names from multiple events",
		},
		{
			name: "SessionCustomVariables_EventsWithoutUnderscoreCvarDoNotErasePriorValues",
			buildHits: func(t *testing.T) columntests.TestHits {
				h1 := columntests.TestHitOne()
				h1.EventName = protocol.PageViewEventType
				h2 := columntests.TestHitOne()
				h2.EventName = protocol.PageViewEventType
				return columntests.TestHits{h1, h2}
			},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "_cvar", `{"1":["Plan","Pro"]}`),
				columntests.EnsureQueryParam(1, "v", "2"),
			},
			expected: []any{
				map[string]any{"name": "Plan", "value": "Pro"},
			},
			description: "Events without _cvar keep previously merged values",
		},
		{
			name:        "SessionCustomVariables_EmptySessionResultIsNil",
			buildHits:   buildPageViewHit,
			expected:    nil,
			description: "Returns nil when no valid _cvar values exist in session",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				tc.buildHits(t),
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					require.NotEmpty(t, whd.WriteCalls, "expected at least one warehouse write call")
					require.NotEmpty(t, whd.WriteCalls[0].Records, "expected at least one record written")
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record["session_custom_variables"], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}
