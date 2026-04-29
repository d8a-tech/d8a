package columntests

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageViewSessionCoreColumns(t *testing.T) {
	proto := ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry())

	testCases := []struct {
		name     string
		hits     TestHits
		field    string
		expected any
	}{
		{
			name:     "entry location no page views",
			hits:     TestHits{newPageViewTestHit("scroll", 0, "https://a", "A")},
			field:    "session_entry_page_location",
			expected: nil,
		},
		{
			name: "entry location first page view",
			hits: TestHits{
				newPageViewTestHit("scroll", 0, "https://x", "X"),
				newPageViewTestHit("page_view", 1, "https://entry", "Entry"),
			},
			field:    "session_entry_page_location",
			expected: "https://entry",
		},
		{
			name: "second location",
			hits: TestHits{
				newPageViewTestHit("page_view", 0, "https://p1", "P1"),
				newPageViewTestHit("page_view", 1, "https://p2", "P2"),
				newPageViewTestHit("page_view", 2, "https://p3", "P3"),
			},
			field:    "session_second_page_location",
			expected: "https://p2",
		},
		{
			name: "exit title",
			hits: TestHits{
				newPageViewTestHit("page_view", 0, "https://p1", "P1"),
				newPageViewTestHit("click", 1, "https://p2", "Ignored"),
				newPageViewTestHit("page_view", 2, "https://p3", "Exit"),
			},
			field:    "session_exit_page_title",
			expected: "Exit",
		},
		{
			name: "first page view utm",
			hits: TestHits{
				newPageViewTestHit("scroll", 0, "https://x?utm_campaign=ignored", "X"),
				newPageViewTestHit("page_view", 1, "https://entry?utm_campaign=summer", "Entry"),
			},
			field:    "session_utm_campaign",
			expected: "summer",
		},
		{
			name: "first page view click id",
			hits: TestHits{
				newPageViewTestHit("scroll", 0, "https://x?gclid=ignored", "X"),
				newPageViewTestHit("page_view", 1, "https://entry?gclid=abc123", "Entry"),
			},
			field:    "session_click_id_gclid",
			expected: "abc123",
		},
		{
			name: "total page views zero",
			hits: TestHits{
				newPageViewTestHit("scroll", 0, "https://a", "A"),
				newPageViewTestHit("click", 1, "https://b", "B"),
			},
			field:    "session_total_page_views",
			expected: 0,
		},
		{
			name: "total page views multiple",
			hits: TestHits{
				newPageViewTestHit("page_view", 0, "https://a", "A"),
				newPageViewTestHit("scroll", 1, "https://a", "A"),
				newPageViewTestHit("page_view", 2, "https://b", "B"),
			},
			field:    "session_total_page_views",
			expected: 2,
		},
		{
			name: "unique page views by location",
			hits: TestHits{
				newPageViewTestHit("page_view", 0, "https://a", "A"),
				newPageViewTestHit("page_view", 1, "https://a", "A"),
				newPageViewTestHit("page_view", 2, "https://b", "B"),
			},
			field:    "session_unique_page_views",
			expected: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ColumnTestCase(t, tc.hits, func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
				require.NoError(t, closeErr)
				record := whd.WriteCalls[0].Records[0]
				assert.Equal(t, tc.expected, record[tc.field])
			}, proto)
		})
	}
}
