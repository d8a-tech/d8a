//nolint:funlen,lll // test code
package matomo

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatomoSessionPageColumns(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	type testCase struct {
		name        string
		hits        columntests.TestHits
		cfg         []columntests.CaseConfigFunc
		fieldName   string
		expected    any
		description string
	}

	testCases := []testCase{
		{
			name: "EntryPageLocation_SinglePageView",
			hits: columntests.TestHits{testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "url", "https://example.com/entry"),
			},
			fieldName:   "session_entry_page_location",
			expected:    "https://example.com/entry",
			description: "Entry page location from single page view",
		},
		{
			name: "EntryPageLocation_MultiplePageViews",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "url", "https://example.com/first"),
				columntests.EnsureQueryParam(1, "url", "https://example.com/second"),
			},
			fieldName:   "session_entry_page_location",
			expected:    "https://example.com/first",
			description: "Entry page location from first of multiple page views",
		},
		{
			name: "EntryPageLocation_NoPageViews",
			hits: columntests.TestHits{testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureEventName(0, "outlink"),
				columntests.EnsureQueryParam(0, "link", "https://ext.com"),
			},
			fieldName:   "session_entry_page_location",
			expected:    nil,
			description: "Entry page location is nil when no page views",
		},
		{
			name: "SecondPageLocation_SinglePageView",
			hits: columntests.TestHits{testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "url", "https://example.com/only"),
			},
			fieldName:   "session_second_page_location",
			expected:    nil,
			description: "Second page location is nil with only one page view",
		},
		{
			name: "SecondPageLocation_TwoPageViews",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "url", "https://example.com/first"),
				columntests.EnsureQueryParam(1, "url", "https://example.com/second"),
			},
			fieldName:   "session_second_page_location",
			expected:    "https://example.com/second",
			description: "Second page location from second page view",
		},
		{
			name: "ExitPageLocation_SinglePageView",
			hits: columntests.TestHits{testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "url", "https://example.com/exit"),
			},
			fieldName:   "session_exit_page_location",
			expected:    "https://example.com/exit",
			description: "Exit page location from single page view",
		},
		{
			name: "ExitPageLocation_MultiplePageViews",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "url", "https://example.com/first"),
				columntests.EnsureQueryParam(1, "url", "https://example.com/last"),
			},
			fieldName:   "session_exit_page_location",
			expected:    "https://example.com/last",
			description: "Exit page location from last of multiple page views",
		},
		{
			name: "ExitPageLocation_PageViewThenOutlink",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "url", "https://example.com/page"),
				columntests.EnsureEventName(1, "outlink"),
				columntests.EnsureQueryParam(1, "link", "https://ext.com"),
			},
			fieldName:   "session_exit_page_location",
			expected:    "https://example.com/page",
			description: "Exit page location ignores non-page-view events at end",
		},
		{
			name: "EntryPageTitle_SinglePageView",
			hits: columntests.TestHits{testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "action_name", "Entry Page"),
			},
			fieldName:   "session_entry_page_title",
			expected:    "Entry Page",
			description: "Entry page title from single page view",
		},
		{
			name: "EntryPageTitle_NoPageViews",
			hits: columntests.TestHits{testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureEventName(0, "outlink"),
				columntests.EnsureQueryParam(0, "link", "https://ext.com"),
			},
			fieldName:   "session_entry_page_title",
			expected:    nil,
			description: "Entry page title is nil when no page views",
		},
		{
			name: "SecondPageTitle_SinglePageView",
			hits: columntests.TestHits{testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "action_name", "Only Page"),
			},
			fieldName:   "session_second_page_title",
			expected:    nil,
			description: "Second page title is nil with only one page view",
		},
		{
			name: "SecondPageTitle_TwoPageViews",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "action_name", "First Page"),
				columntests.EnsureQueryParam(1, "action_name", "Second Page"),
			},
			fieldName:   "session_second_page_title",
			expected:    "Second Page",
			description: "Second page title from second page view",
		},
		{
			name: "ExitPageTitle_SinglePageView",
			hits: columntests.TestHits{testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "action_name", "Exit Page"),
			},
			fieldName:   "session_exit_page_title",
			expected:    "Exit Page",
			description: "Exit page title from single page view",
		},
		{
			name: "ExitPageTitle_MultiplePageViews",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "action_name", "First"),
				columntests.EnsureQueryParam(1, "action_name", "Last"),
			},
			fieldName:   "session_exit_page_title",
			expected:    "Last",
			description: "Exit page title from last of multiple page views",
		},

		// Additional cases to exceed minimum test count and cover mixed event ordering.
		{
			name: "EntryPageLocation_PageViewAfterOutlink",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureEventName(0, "outlink"),
				columntests.EnsureQueryParam(0, "link", "https://ext.com"),
				columntests.EnsureQueryParam(1, "url", "https://example.com/landing"),
			},
			fieldName:   "session_entry_page_location",
			expected:    "https://example.com/landing",
			description: "Entry page location uses the first page view even when preceded by an outlink",
		},
		{
			name: "ExitPageLocation_OutlinkThenPageView",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureEventName(0, "outlink"),
				columntests.EnsureQueryParam(0, "link", "https://ext.com"),
				columntests.EnsureQueryParam(1, "url", "https://example.com/last"),
			},
			fieldName:   "session_exit_page_location",
			expected:    "https://example.com/last",
			description: "Exit page location uses the last page view even when preceded by an outlink",
		},
		{
			name: "EntryPageTitle_PageViewAfterOutlink",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureEventName(0, "outlink"),
				columntests.EnsureQueryParam(0, "link", "https://ext.com"),
				columntests.EnsureQueryParam(1, "action_name", "Landing"),
			},
			fieldName:   "session_entry_page_title",
			expected:    "Landing",
			description: "Entry page title uses the first page view even when preceded by an outlink",
		},
		{
			name: "ExitPageTitle_PageViewThenOutlink",
			hits: columntests.TestHits{testHitOne(), testHitOne()},
			cfg: []columntests.CaseConfigFunc{
				columntests.EnsureQueryParam(0, "action_name", "Page"),
				columntests.EnsureEventName(1, "outlink"),
				columntests.EnsureQueryParam(1, "link", "https://ext.com"),
			},
			fieldName:   "session_exit_page_title",
			expected:    "Page",
			description: "Exit page title ignores non-page-view events at end",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			cfgs := append(tc.cfg, columntests.EnsureQueryParam(0, "v", "2")) //nolint:gocritic // test code
			columntests.ColumnTestCase(
				t,
				tc.hits,
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					require.NotEmpty(t, whd.WriteCalls)
					require.NotEmpty(t, whd.WriteCalls[0].Records)
					record := whd.WriteCalls[0].Records[0]
					assert.Equal(t, tc.expected, record[tc.fieldName], tc.description)
				},
				proto,
				cfgs...,
			)
		})
	}
}
