//nolint:funlen,lll // test code
package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoSearchColumns(t *testing.T) {
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
		description string
	}

	testCases := []testCase{
		{
			name:        "EventParamsSearchTerm_Valid",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search", "d8a docs")},
			fieldName:   "params_search_term",
			expected:    "d8a docs",
			description: "Valid site-search term via search query parameter",
		},
		{
			name:        "EventParamsSearchTerm_Empty",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search", "")},
			fieldName:   "params_search_term",
			expected:    nil,
			description: "Returns nil when search parameter is empty",
		},
		{
			name:        "EventParamsSearchTerm_Absent",
			buildHits:   single(buildPageViewHit),
			fieldName:   "params_search_term",
			expected:    nil,
			description: "Returns nil when search parameter is absent",
		},
		{
			name:        "EventParamsSearchKeyword_Valid",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search", "d8a docs")},
			fieldName:   "params_search_keyword",
			expected:    "d8a docs",
			description: "Valid site-search keyword via search query parameter",
		},
		{
			name:        "EventParamsSearchKeyword_Empty",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search", "")},
			fieldName:   "params_search_keyword",
			expected:    nil,
			description: "Returns nil when search parameter is empty",
		},
		{
			name:        "EventParamsSearchKeyword_Absent",
			buildHits:   single(buildPageViewHit),
			fieldName:   "params_search_keyword",
			expected:    nil,
			description: "Returns nil when search parameter is absent",
		},
		{
			name:        "EventParamsSearchCategory_Valid",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search_cat", "Docs")},
			fieldName:   "params_search_category",
			expected:    "Docs",
			description: "Valid search category via search_cat query parameter",
		},
		{
			name:        "EventParamsSearchCategory_Empty",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search_cat", "")},
			fieldName:   "params_search_category",
			expected:    nil,
			description: "Returns nil when search_cat parameter is empty",
		},
		{
			name:        "EventParamsSearchCategory_Absent",
			buildHits:   single(buildPageViewHit),
			fieldName:   "params_search_category",
			expected:    nil,
			description: "Returns nil when search_cat parameter is absent",
		},
		{
			name:        "EventParamsSearchCount_ValidInteger",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search_count", "7")},
			fieldName:   "params_search_count",
			expected:    int64(7),
			description: "Valid search count as integer via search_count query parameter",
		},
		{
			name:        "EventParamsSearchCount_ValidLargeInteger",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search_count", "1000")},
			fieldName:   "params_search_count",
			expected:    int64(1000),
			description: "Valid large search count as integer",
		},
		{
			name:        "EventParamsSearchCount_Invalid",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search_count", "not_a_number")},
			fieldName:   "params_search_count",
			expected:    nil,
			description: "Returns nil when search_count is not parseable as integer",
		},
		{
			name:        "EventParamsSearchCount_Empty",
			buildHits:   single(buildPageViewHit),
			cfg:         []columntests.CaseConfigFunc{columntests.EnsureQueryParam(0, "search_count", "")},
			fieldName:   "params_search_count",
			expected:    nil,
			description: "Returns nil when search_count parameter is empty",
		},
		{
			name:        "EventParamsSearchCount_Absent",
			buildHits:   single(buildPageViewHit),
			fieldName:   "params_search_count",
			expected:    nil,
			description: "Returns nil when search_count parameter is absent",
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
					require.NotEmpty(t, whd.WriteCalls)
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
				},
				proto,
				tc.cfg...,
			)
		})
	}
}
