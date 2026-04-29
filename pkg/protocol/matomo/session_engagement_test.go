// nolint:funlen,lll // test code
package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoSessionEngagementColumns(t *testing.T) {
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
			name: "TotalPurchases_OrderDetected",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "idgoal", "0"),
				columntests.EnsureQueryParam(0, "ec_id", "ord_123"),
				columntests.EnsureEventName(0, "ecommerce_order"),
			},
			fieldName:   "session_total_purchases",
			expected:    1,
			description: "An ecommerce order with idgoal=0 and ec_id counts as one purchase",
		},
		{
			name: "TotalPurchases_NonOrderGoalNotCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "idgoal", "1"),
			},
			fieldName:   "session_total_purchases",
			expected:    0,
			description: "A non-ecommerce goal does not count as a purchase",
		},
		{
			name: "TotalGoalConversions_GoalCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "idgoal", "7"),
				columntests.EnsureEventName(0, "goal_conversion"),
			},
			fieldName:   "session_total_goal_conversions",
			expected:    1,
			description: "A goal conversion event counts as one goal conversion",
		},
		{
			name: "TotalGoalConversions_NoGoalNoCount",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "idgoal", "0"),
				columntests.EnsureQueryParam(0, "ec_id", "ord_123"),
				columntests.EnsureEventName(0, "ecommerce_order"),
			},
			fieldName:   "session_total_goal_conversions",
			expected:    0,
			description: "An ecommerce order does not count as a goal conversion",
		},
		{
			name:        "TotalScrolls_AlwaysNull",
			cfg:         nil,
			fieldName:   "session_total_scrolls",
			expected:    nil,
			description: "Scroll tracking is not supported in Matomo and always returns null",
		},
		{
			name: "TotalOutboundClicks_OutlinkCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "link", "https://ext.com"),
				columntests.EnsureEventName(0, "outlink"),
			},
			fieldName:   "session_total_outbound_clicks",
			expected:    1,
			description: "An outlink click counts as one outbound click",
		},
		{
			name:        "TotalOutboundClicks_NoLinkNoCount",
			cfg:         nil,
			fieldName:   "session_total_outbound_clicks",
			expected:    0,
			description: "A page view without a link param does not count as an outbound click",
		},
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
		{
			name: "TotalVideoEngagements_VideoPlayCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "ma_id", "vid_1"),
				columntests.EnsureQueryParam(0, "ma_mt", "video"),
				columntests.EnsureEventName(0, "video_play"),
			},
			fieldName:   "session_total_video_engagements",
			expected:    1,
			description: "A video play event counts as one video engagement",
		},
		{
			name: "TotalVideoEngagements_AudioNotCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "ma_id", "aud_1"),
				columntests.EnsureQueryParam(0, "ma_mt", "audio"),
			},
			fieldName:   "session_total_video_engagements",
			expected:    0,
			description: "An audio media event does not count as a video engagement",
		},
		{
			name:        "TotalVideoEngagements_NoMediaNoCount",
			cfg:         nil,
			fieldName:   "session_total_video_engagements",
			expected:    0,
			description: "A page view without media params does not count as a video engagement",
		},
		{
			name: "TotalFileDownloads_DownloadCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "download", "https://cdn.com/f.pdf"),
				columntests.EnsureEventName(0, "download"),
			},
			fieldName:   "session_total_file_downloads",
			expected:    1,
			description: "A download event counts as one file download",
		},
		{
			name:        "TotalFileDownloads_NoDownloadNoCount",
			cfg:         nil,
			fieldName:   "session_total_file_downloads",
			expected:    0,
			description: "A page view without a download param does not count as a file download",
		},
		{
			name: "UniqueFileDownloads_DownloadCounted",
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "download", "https://cdn.com/f.pdf"),
				columntests.EnsureEventName(0, "download"),
			},
			fieldName:   "session_unique_file_downloads",
			expected:    1,
			description: "A download event counts as one unique file download",
		},
		{
			name:        "UniqueFileDownloads_NoDownloadNoCount",
			cfg:         nil,
			fieldName:   "session_unique_file_downloads",
			expected:    0,
			description: "A page view without a download param does not count as a unique file download",
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
