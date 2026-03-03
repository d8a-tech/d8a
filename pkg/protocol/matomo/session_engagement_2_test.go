// nolint:funlen,lll // test code
package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoSessionEngagement2Columns(t *testing.T) {
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
			name: "UniqueOutboundClicks_OutlinkCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "link", "https://ext.com"),
				columntests.EnsureEventName(0, "outlink"),
			},
			fieldName:   "session_unique_outbound_clicks",
			expected:    1,
			description: "An outlink click counts as one unique outbound click",
		},
		{
			name:        "UniqueOutboundClicks_NoLinkNoCount",
			cfg:         nil,
			fieldName:   "session_unique_outbound_clicks",
			expected:    0,
			description: "A page view without a link param does not count as a unique outbound click",
		},
		{
			name: "TotalSiteSearches_SearchCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "search", "golang"),
				columntests.EnsureEventName(0, "site_search"),
			},
			fieldName:   "session_total_site_searches",
			expected:    1,
			description: "A site search event counts as one site search",
		},
		{
			name:        "TotalSiteSearches_NoSearchNoCount",
			cfg:         nil,
			fieldName:   "session_total_site_searches",
			expected:    0,
			description: "A page view without a search param does not count as a site search",
		},
		{
			name: "UniqueSiteSearches_SearchCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "search", "golang"),
				columntests.EnsureEventName(0, "site_search"),
			},
			fieldName:   "session_unique_site_searches",
			expected:    1,
			description: "A site search event counts as one unique site search",
		},
		{
			name:        "UniqueSiteSearches_NoSearchNoCount",
			cfg:         nil,
			fieldName:   "session_unique_site_searches",
			expected:    0,
			description: "A page view without a search param does not count as a unique site search",
		},
		{
			name:        "TotalFormInteractions_AlwaysNull",
			cfg:         nil,
			fieldName:   "session_total_form_interactions",
			expected:    nil,
			description: "Form interaction tracking is not supported in Matomo and always returns null",
		},
		{
			name:        "UniqueFormInteractions_AlwaysNull",
			cfg:         nil,
			fieldName:   "session_unique_form_interactions",
			expected:    nil,
			description: "Unique form interaction tracking is not supported in Matomo and always returns null",
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
