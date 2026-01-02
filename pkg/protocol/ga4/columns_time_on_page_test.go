package ga4

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// simpleHitsWithTime creates hits with event types, page locations, and timestamps
// Each hit is [event_type, page_location, page_title, time_offset_seconds]
func simpleHitsWithTime(h [][4]any) columntests.TestHits {
	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	var allHits = make([]*hits.Hit, len(h))
	for i, hit := range h {
		allHits[i] = hits.New()
		eventType, ok := hit[0].(string)
		if !ok {
			panic("event type must be string")
		}
		allHits[i].EventName = eventType
		allHits[i].MustParsedRequest().QueryParams.Add("en", eventType)
		if hit[1] != nil {
			pageLocation, ok := hit[1].(string)
			if !ok {
				panic("page location must be string")
			}
			allHits[i].MustParsedRequest().QueryParams.Add("dl", pageLocation)
		}
		if hit[2] != nil {
			pageTitle, ok := hit[2].(string)
			if !ok {
				panic("page title must be string")
			}
			allHits[i].MustParsedRequest().QueryParams.Add("dt", pageTitle)
		}
		offsetSeconds, ok := hit[3].(int)
		if !ok {
			panic("time offset must be int")
		}
		allHits[i].MustParsedRequest().ServerReceivedTime = baseTime.Add(time.Duration(offsetSeconds) * time.Second)
	}
	return allHits
}

// nolint:funlen // test code
func TestTimeOnPage(t *testing.T) {
	var testCases = []struct {
		name        string
		hits        columntests.TestHits
		expected    []any
		description string
	}{
		{
			name: "Session starting with page_view - basic example",
			hits: simpleHitsWithTime([][4]any{
				{"page_view", "https://example.com/page1", "Page 1", 0},
				{"scroll", "https://example.com/page1", "Page 1", 1},
				{"scroll", "https://example.com/page1", "Page 1", 2},
				{"page_view", "https://example.com/page2", "Page 2", 10},
				{"scroll", "https://example.com/page2", "Page 2", 12},
			}),
			expected: []any{int64(10), int64(10), int64(10), int64(2), int64(2)},
			description: "First page_view at 0s, next at 10s (10s time_on_page), " +
				"second page_view at 10s, last event at 12s (2s time_on_page)",
		},
		{
			name: "Session not starting with page_view - treat first event as page_view",
			hits: simpleHitsWithTime([][4]any{
				{"scroll", "https://example.com/page1", "Page 1", 0},
				{"click", "https://example.com/page1", "Page 1", 1},
				{"page_view", "https://example.com/page2", "Page 2", 5},
				{"scroll", "https://example.com/page2", "Page 2", 6},
			}),
			expected: []any{int64(5), int64(5), int64(1), int64(1)},
			description: "First event (scroll) treated as page_view at 0s, next page_view at 5s (5s time_on_page), " +
				"second page_view at 5s, last event at 6s (1s time_on_page)",
		},
		{
			name: "Session ending with page_view - time_on_page is 0",
			hits: simpleHitsWithTime([][4]any{
				{"page_view", "https://example.com/page1", "Page 1", 0},
				{"scroll", "https://example.com/page1", "Page 1", 1},
				{"page_view", "https://example.com/page2", "Page 2", 10},
			}),
			expected: []any{int64(10), int64(10), int64(0)},
			description: "First page_view at 0s, next at 10s (10s time_on_page), " +
				"second page_view at 10s with no next page_view, so time_on_page = 0",
		},
		{
			name: "Session not ending with page_view - use last event time",
			hits: simpleHitsWithTime([][4]any{
				{"page_view", "https://example.com/page1", "Page 1", 0},
				{"scroll", "https://example.com/page1", "Page 1", 1},
				{"page_view", "https://example.com/page2", "Page 2", 10},
				{"scroll", "https://example.com/page2", "Page 2", 12},
				{"click", "https://example.com/page2", "Page 2", 15},
			}),
			expected: []any{int64(10), int64(10), int64(5), int64(5), int64(5)},
			description: "First page_view at 0s, next at 10s (10s time_on_page), " +
				"second page_view at 10s, last event at 15s (5s time_on_page)",
		},
		{
			name: "Session with single event - page_view",
			hits: simpleHitsWithTime([][4]any{
				{"page_view", "https://example.com/page1", "Page 1", 0},
			}),
			expected:    []any{int64(0)},
			description: "Single page_view event, no next page_view, so time_on_page = 0",
		},
		{
			name: "Session with single event - not page_view",
			hits: simpleHitsWithTime([][4]any{
				{"scroll", "https://example.com/page1", "Page 1", 0},
			}),
			expected:    []any{int64(0)},
			description: "Single non-page_view event treated as page_view, no next page_view, so time_on_page = 0",
		},
		{
			name: "Multiple page_views with various events",
			hits: simpleHitsWithTime([][4]any{
				{"page_view", "https://example.com/page1", "Page 1", 0},
				{"scroll", "https://example.com/page1", "Page 1", 2},
				{"click", "https://example.com/page1", "Page 1", 4},
				{"page_view", "https://example.com/page2", "Page 2", 10},
				{"scroll", "https://example.com/page2", "Page 2", 11},
				{"page_view", "https://example.com/page3", "Page 3", 15},
				{"scroll", "https://example.com/page3", "Page 3", 16},
				{"click", "https://example.com/page3", "Page 3", 18},
			}),
			expected:    []any{int64(10), int64(10), int64(10), int64(5), int64(5), int64(3), int64(3), int64(3)},
			description: "First page: 0-10s (10s), second page: 10-15s (5s), third page: 15-18s (3s)",
		},
		{
			name: "Session starting with non-pv, ending with pv",
			hits: simpleHitsWithTime([][4]any{
				{"scroll", "https://example.com/page1", "Page 1", 0},
				{"click", "https://example.com/page1", "Page 1", 2},
				{"page_view", "https://example.com/page2", "Page 2", 5},
				{"scroll", "https://example.com/page2", "Page 2", 6},
				{"page_view", "https://example.com/page3", "Page 3", 10},
			}),
			expected: []any{int64(5), int64(5), int64(5), int64(5), int64(0)},
			description: "First event treated as page_view at 0s, next at 5s (5s), " +
				"second page_view at 5s, next at 10s (5s), third page_view at 10s (0s)",
		},
		{
			name: "Session with no page_views at all",
			hits: simpleHitsWithTime([][4]any{
				{"scroll", "https://example.com/page1", "Page 1", 0},
				{"click", "https://example.com/page1", "Page 1", 2},
				{"scroll", "https://example.com/page1", "Page 1", 5},
			}),
			expected:    []any{int64(5), int64(5), int64(5)},
			description: "No page_views, first event treated as page_view at 0s, last event at 5s (5s time_on_page for all)",
		},
		{
			name: "Consecutive page_views",
			hits: simpleHitsWithTime([][4]any{
				{"page_view", "https://example.com/page1", "Page 1", 0},
				{"page_view", "https://example.com/page2", "Page 2", 5},
				{"page_view", "https://example.com/page3", "Page 3", 10},
			}),
			expected:    []any{int64(5), int64(5), int64(0)},
			description: "First page_view at 0s, next at 5s (5s), second at 5s, next at 10s (5s), third at 10s (0s)",
		},
		{
			name: "Page_view followed immediately by another page_view",
			hits: simpleHitsWithTime([][4]any{
				{"page_view", "https://example.com/page1", "Page 1", 0},
				{"page_view", "https://example.com/page2", "Page 2", 0},
				{"scroll", "https://example.com/page2", "Page 2", 1},
			}),
			expected:    []any{int64(0), int64(1), int64(1)},
			description: "First page_view at 0s, next at 0s (0s), second at 0s, last event at 1s (1s)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			columntests.ColumnTestCase(
				t,
				tc.hits,
				func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					// when + then
					require.NoError(t, closeErr)
					require.GreaterOrEqual(t, len(whd.WriteCalls[0].Records), len(tc.expected),
						"Not enough records in output")
					writeResult := []any{}
					for _, record := range whd.WriteCalls[0].Records {
						writeResult = append(writeResult, record["time_on_page"])
					}
					assert.Equal(t, tc.expected, writeResult, tc.description)
				},
				NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
			)
		})
	}
}
