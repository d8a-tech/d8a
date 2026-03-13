package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nolint:funlen,lll // test code
func TestMatomoContentColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	buildPageViewHit := func(_ *testing.T) *hits.Hit {
		hit := columntests.TestHitOne()
		hit.EventName = "page_view"
		return hit
	}

	single := func(build func(*testing.T) *hits.Hit) func(*testing.T) columntests.TestHits {
		return func(t *testing.T) columntests.TestHits {
			return columntests.TestHits{build(t)}
		}
	}

	type testCase struct {
		name        string
		buildHits   func(t *testing.T) columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		expectNoIO  bool
		description string
	}

	mergeCases := func(groups ...[]testCase) []testCase {
		total := 0
		for _, g := range groups {
			total += len(g)
		}
		out := make([]testCase, 0, total)
		for _, g := range groups {
			out = append(out, g...)
		}
		return out
	}

	testCases := mergeCases(
		[]testCase{

			{
				name:        "EventParamsContentInteraction_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "c_i", "click")},
				fieldName:   "params_content_interaction",
				expected:    "click",
				description: "Valid params content interaction via c_i query parameter",
			},
			{
				name:        "EventParamsContentInteraction_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_content_interaction",
				expected:    nil,
				description: "Returns nil when c_i parameter is absent",
			},
			{
				name:        "EventParamsContentName_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "c_n", "Hero Banner")},
				fieldName:   "params_content_name",
				expected:    "Hero Banner",
				description: "Valid params content name via c_n query parameter",
			},
			{
				name:        "EventParamsContentName_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_content_name",
				expected:    nil,
				description: "Returns nil when c_n parameter is absent",
			},
			{
				name:        "EventParamsContentPiece_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "c_p", "/assets/banner.jpg")},
				fieldName:   "params_content_piece",
				expected:    "/assets/banner.jpg",
				description: "Valid params content piece via c_p query parameter",
			},
			{
				name:        "EventParamsContentPiece_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_content_piece",
				expected:    nil,
				description: "Returns nil when c_p parameter is absent",
			},
			{
				name:        "EventParamsContentTarget_Valid",
				buildHits:   single(buildPageViewHit),
				cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "c_t", "https://example.com/landing")},
				fieldName:   "params_content_target",
				expected:    "https://example.com/landing",
				description: "Valid params content target via c_t query parameter",
			},
			{
				name:        "EventParamsContentTarget_Absent",
				buildHits:   single(buildPageViewHit),
				fieldName:   "params_content_target",
				expected:    nil,
				description: "Returns nil when c_t parameter is absent",
			},
		},
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				tc.buildHits(t),
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					if tc.expectNoIO {
						require.Empty(t, whd.WriteCalls, "expected no warehouse write calls")
						return
					}
					require.NotEmpty(t, whd.WriteCalls, "expected at least one warehouse write call")
					require.NotEmpty(t, whd.WriteCalls[0].Records, "expected at least one record written")
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
				},
				proto,
				append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2"))...,
			)
		})
	}
}

func TestMatomoSessionContentColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	type testCase struct {
		name        string
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		description string
	}

	testCases := []testCase{
		{
			name: "TotalContentImpressions_ContentNameCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "c_n", "Hero Banner"),
				columntests.EnsureQueryParam(0, "c_p", "/assets/banner.jpg"),
				columntests.EnsureQueryParam(0, "c_t", "https://example.com/landing"),
				columntests.EnsureEventName(0, "content_impression"),
			},
			fieldName:   "session_total_content_impressions",
			expected:    1,
			description: "A content impression event counts as one content impression",
		},
		{
			name: "TotalContentImpressions_NoContentNameNoCount",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "c_i", "click"),
				columntests.EnsureEventName(0, "content_interaction"),
			},
			fieldName:   "session_total_content_impressions",
			expected:    0,
			description: "A content interaction event does not count as a content impression",
		},
		{
			name: "TotalContentInteractions_ContentInteractionCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "c_i", "click"),
				columntests.EnsureQueryParam(0, "c_n", "Hero Banner"),
				columntests.EnsureEventName(0, "content_interaction"),
			},
			fieldName:   "session_total_content_interactions",
			expected:    1,
			description: "A content interaction event counts as one content interaction",
		},
		{
			name: "TotalContentInteractions_NoContentInteractionNoCount",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "c_n", "Hero Banner"),
				columntests.EnsureEventName(0, "content_impression"),
			},
			fieldName:   "session_total_content_interactions",
			expected:    0,
			description: "A content impression event does not count as a content interaction",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			cfgs := append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2")) //nolint:gocritic // test code
			columntests.ColumnTestCase(
				t,
				columntests.TestHits{testHitOne()},
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					require.NotEmpty(t, whd.WriteCalls, "expected at least one warehouse write call")
					require.NotEmpty(t, whd.WriteCalls[0].Records, "expected at least one record")
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
				},
				proto,
				cfgs...,
			)
		})
	}
}
