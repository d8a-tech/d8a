package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoCustomDimensionsColumn(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) columntests.TestHits {
		hit := columntests.TestHitOne()
		hit.EventName = pageViewEventType
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
			name:      "EventCustomDimensions_ValidSingle",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{columntests.EnsureQueryParam(
				0,
				"dimension1",
				"OS",
			)},
			expected: []any{
				map[string]any{"slot": int64(1), "value": "OS"},
			},
			description: "Parses one custom dimension from dimensionN query parameter",
		},
		{
			name:      "EventCustomDimensions_ValidMultiple",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "dimension1", "one"),
				columntests.EnsureQueryParam(0, "dimension2", "two"),
			},
			expected: []any{
				map[string]any{"slot": int64(1), "value": "one"},
				map[string]any{"slot": int64(2), "value": "two"},
			},
			description: "Parses multiple custom dimensions",
		},
		{
			name:      "EventCustomDimensions_DeterministicOrdering",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "dimension10", "ten"),
				columntests.EnsureQueryParam(0, "dimension2", "two"),
				columntests.EnsureQueryParam(0, "dimension1", "one"),
			},
			expected: []any{
				map[string]any{"slot": int64(1), "value": "one"},
				map[string]any{"slot": int64(2), "value": "two"},
				map[string]any{"slot": int64(10), "value": "ten"},
			},
			description: "Sorts custom dimensions by numeric slot ascending",
		},
		{
			name:        "EventCustomDimensions_Absent",
			buildHits:   buildPageViewHit,
			expected:    []any(nil),
			description: "Returns nil when no dimensionN query params are present",
		},
		{
			name:      "EventCustomDimensions_SkipMalformedEntries",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "dimension1", "one"),
				columntests.EnsureQueryParam(0, "dimensionX", "x"),
				columntests.EnsureQueryParam(0, "dimension", "missing-slot"),
				columntests.EnsureQueryParam(0, "dimension3", ""),
			},
			expected: []any{
				map[string]any{"slot": int64(1), "value": "one"},
			},
			description: "Skips malformed custom dimension entries while keeping valid ones",
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
					assert.Equal(t, tc.expected, record["custom_dimensions"], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}

func TestMatomoSessionCustomDimensionsColumn(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) columntests.TestHits {
		hit := columntests.TestHitOne()
		hit.EventName = pageViewEventType
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
			name:      "SessionCustomDimensions_OneEvent",
			buildHits: buildPageViewHit,
			cfg: []columntests.CaseConfigFunc{columntests.EnsureQueryParam(
				0,
				"dimension1",
				"one",
			)},
			expected: []any{
				map[string]any{"slot": int64(1), "value": "one"},
			},
			description: "Parses session custom dimensions from one event",
		},
		{
			name: "SessionCustomDimensions_AccumulatesAcrossMultipleEvents",
			buildHits: func(t *testing.T) columntests.TestHits {
				h1 := columntests.TestHitOne()
				h1.EventName = pageViewEventType
				h2 := columntests.TestHitOne()
				h2.EventName = pageViewEventType
				return columntests.TestHits{h1, h2}
			},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "dimension1", "one"),
				columntests.EnsureQueryParam(1, "dimension2", "two"),
				columntests.EnsureQueryParam(1, "v", "2"),
			},
			expected: []any{
				map[string]any{"slot": int64(1), "value": "one"},
				map[string]any{"slot": int64(2), "value": "two"},
			},
			description: "Accumulates slots from all events and returns deterministic order",
		},
		{
			name: "SessionCustomDimensions_LaterEventOverridesEarlierValue",
			buildHits: func(t *testing.T) columntests.TestHits {
				h1 := columntests.TestHitOne()
				h1.EventName = pageViewEventType
				h2 := columntests.TestHitOne()
				h2.EventName = pageViewEventType
				return columntests.TestHits{h1, h2}
			},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "dimension1", "old"),
				columntests.EnsureQueryParam(1, "dimension1", "new"),
				columntests.EnsureQueryParam(1, "v", "2"),
			},
			expected: []any{
				map[string]any{"slot": int64(1), "value": "new"},
			},
			description: "Uses later event value when slots conflict",
		},
		{
			name: "SessionCustomDimensions_EventsWithoutDimensionsDoNotErasePriorValues",
			buildHits: func(t *testing.T) columntests.TestHits {
				h1 := columntests.TestHitOne()
				h1.EventName = pageViewEventType
				h2 := columntests.TestHitOne()
				h2.EventName = pageViewEventType
				return columntests.TestHits{h1, h2}
			},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "dimension3", "value3"),
				columntests.EnsureQueryParam(1, "v", "2"),
			},
			expected: []any{
				map[string]any{"slot": int64(3), "value": "value3"},
			},
			description: "Events without dimensions keep previously merged slot values",
		},
		{
			name:        "SessionCustomDimensions_EmptySessionResultIsNil",
			buildHits:   buildPageViewHit,
			expected:    nil,
			description: "Returns nil when no valid dimension values exist in session",
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
					assert.Equal(t, tc.expected, record["session_custom_dimensions"], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}
