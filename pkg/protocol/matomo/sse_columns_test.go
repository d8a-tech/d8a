//nolint:funlen,lll // test code
package matomo

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildMatomoHitsWithTime creates Matomo hits with event names, page URLs, and timestamps.
// Each entry is [event_type, page_url, time_offset_seconds].
func buildMatomoHitsWithTime(h [][3]any) columntests.TestHits {
	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	allHits := make([]*hits.Hit, len(h))
	for i, hit := range h {
		allHits[i] = testHitOne()
		eventType, ok := hit[0].(string)
		if !ok {
			panic("event type must be string")
		}
		allHits[i].EventName = eventType
		allHits[i].MustParsedRequest().QueryParams.Set("v", "2")
		if hit[1] != nil {
			pageURL, ok := hit[1].(string)
			if !ok {
				panic("page URL must be string")
			}
			allHits[i].MustParsedRequest().QueryParams.Set("url", pageURL)
		}
		offsetSeconds, ok := hit[2].(int)
		if !ok {
			panic("time offset must be int")
		}
		allHits[i].MustParsedRequest().ServerReceivedTime = baseTime.Add(time.Duration(offsetSeconds) * time.Second)
	}
	return allHits
}

// buildMatomoHits creates Matomo hits with event names and page URLs, without custom timestamps.
func buildMatomoHits(h [][2]string) columntests.TestHits {
	allHits := make([]*hits.Hit, len(h))
	for i, hit := range h {
		allHits[i] = testHitOne()
		allHits[i].EventName = hit[0]
		allHits[i].MustParsedRequest().QueryParams.Set("v", "2")
		allHits[i].MustParsedRequest().QueryParams.Set("url", hit[1])
	}
	return allHits
}

// buildMatomoTransitionHits creates Matomo hits with event name, URL, and title.
// Each entry is [event_type, page_url, page_title].
func buildMatomoTransitionHits(h [][3]string) columntests.TestHits {
	allHits := make([]*hits.Hit, len(h))
	for i, hit := range h {
		allHits[i] = testHitOne()
		allHits[i].EventName = hit[0]
		allHits[i].MustParsedRequest().QueryParams.Set("v", "2")
		allHits[i].MustParsedRequest().QueryParams.Set("url", hit[1])
		allHits[i].MustParsedRequest().QueryParams.Set("action_name", hit[2])
	}

	return allHits
}

func TestMatomoValueTransitions(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	var testCases = []struct {
		name        string
		hits        columntests.TestHits
		field       string
		expected    []any
		description string
	}{
		{
			name: "PreviousPageLocation",
			hits: buildMatomoTransitionHits([][3]string{
				{"page_view", "https://example.com/page1", "Page 1"},
				{"page_view", "https://example.com/page2", "Page 2"},
				{"outlink", "https://example.com/page2", "Page 2"},
				{"download", "https://example.com/page2", "Page 2"},
				{"page_view", "https://example.com/page3", "Page 3"},
			}),
			field: "previous_page_location",
			expected: []any{
				nil,
				"https://example.com/page1",
				"https://example.com/page1",
				"https://example.com/page1",
				"https://example.com/page2",
			},
			description: "Previous page location should carry forward across non-page-view events",
		},
		{
			name: "PreviousPageTitle",
			hits: buildMatomoTransitionHits([][3]string{
				{"page_view", "https://example.com/page1", "Page 1"},
				{"page_view", "https://example.com/page2", "Page 2"},
				{"outlink", "https://example.com/page2", "Page 2"},
				{"download", "https://example.com/page2", "Page 2"},
				{"page_view", "https://example.com/page3", "Page 3"},
			}),
			field: "previous_page_title",
			expected: []any{
				nil,
				"Page 1",
				"Page 1",
				"Page 1",
				"Page 2",
			},
			description: "Previous page title should carry forward across non-page-view events",
		},
		{
			name: "NextPageLocation",
			hits: buildMatomoTransitionHits([][3]string{
				{"page_view", "https://example.com/page1", "Page 1"},
				{"page_view", "https://example.com/page2", "Page 2"},
				{"outlink", "https://example.com/page2", "Page 2"},
				{"download", "https://example.com/page2", "Page 2"},
				{"page_view", "https://example.com/page3", "Page 3"},
			}),
			field: "next_page_location",
			expected: []any{
				"https://example.com/page2",
				"https://example.com/page3",
				"https://example.com/page3",
				"https://example.com/page3",
				nil,
			},
			description: "Next page location should be nil for last page view and shared by in-between events",
		},
		{
			name: "NextPageTitle",
			hits: buildMatomoTransitionHits([][3]string{
				{"page_view", "https://example.com/page1", "Page 1"},
				{"page_view", "https://example.com/page2", "Page 2"},
				{"outlink", "https://example.com/page2", "Page 2"},
				{"download", "https://example.com/page2", "Page 2"},
				{"page_view", "https://example.com/page3", "Page 3"},
			}),
			field: "next_page_title",
			expected: []any{
				"Page 2",
				"Page 3",
				"Page 3",
				"Page 3",
				nil,
			},
			description: "Next page title should be nil for last page view and shared by in-between events",
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
					writeResult := make([]any, 0, len(whd.WriteCalls[0].Records))
					for _, record := range whd.WriteCalls[0].Records {
						writeResult = append(writeResult, record[tc.field])
					}
					assert.Equal(t, tc.expected, writeResult, tc.description)
				},
				proto,
			)
		})
	}
}

func TestMatomoSSEIsEntryExitPage(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	var testCases = []struct {
		name        string
		hits        columntests.TestHits
		field       string
		expected    []any
		description string
	}{
		{
			name: "SSEIsEntryPage - no page_view in session",
			hits: buildMatomoHits([][2]string{
				{"outlink", "https://example.com/page1"},
				{"download", "https://example.com/page1"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(0), int64(0)},
			description: "No page_view events, all should be 0",
		},
		{
			name: "SSEIsExitPage - no page_view in session",
			hits: buildMatomoHits([][2]string{
				{"outlink", "https://example.com/page1"},
				{"download", "https://example.com/page1"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(0)},
			description: "No page_view events, all should be 0",
		},
		{
			name: "SSEIsEntryPage - single page_view",
			hits: buildMatomoHits([][2]string{
				{"page_view", "https://example.com/page1"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(1)},
			description: "Single page_view should be marked as entry",
		},
		{
			name: "SSEIsExitPage - single page_view",
			hits: buildMatomoHits([][2]string{
				{"page_view", "https://example.com/page1"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(1)},
			description: "Single page_view should be marked as exit",
		},
		{
			name: "SSEIsEntryPage - session starting with page_view",
			hits: buildMatomoHits([][2]string{
				{"page_view", "https://example.com/page1"},
				{"outlink", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"download", "https://example.com/page2"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(1), int64(0), int64(0), int64(0)},
			description: "First page_view should be marked as entry",
		},
		{
			name: "SSEIsExitPage - session ending with page_view",
			hits: buildMatomoHits([][2]string{
				{"page_view", "https://example.com/page1"},
				{"outlink", "https://example.com/page1"},
				{"download", "https://example.com/page2"},
				{"page_view", "https://example.com/page2"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(0), int64(0), int64(1)},
			description: "Last page_view should be marked as exit",
		},
		{
			name: "SSEIsEntryPage - session not starting with page_view",
			hits: buildMatomoHits([][2]string{
				{"outlink", "https://example.com/page1"},
				{"download", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(0), int64(0), int64(1), int64(0)},
			description: "First page_view (not first event) should be marked as entry",
		},
		{
			name: "SSEIsExitPage - session not ending with page_view",
			hits: buildMatomoHits([][2]string{
				{"page_view", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"outlink", "https://example.com/page2"},
				{"download", "https://example.com/page2"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(1), int64(0), int64(0)},
			description: "Last page_view (not last event) should be marked as exit",
		},
		{
			name: "SSEIsEntryPage - multiple page_views with non-pv events",
			hits: buildMatomoHits([][2]string{
				{"page_view", "https://example.com/page1"},
				{"outlink", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"download", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
				{"outlink", "https://example.com/page3"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(1), int64(0), int64(0), int64(0), int64(0), int64(0)},
			description: "Only first page_view should be marked as entry",
		},
		{
			name: "SSEIsExitPage - multiple page_views with non-pv events",
			hits: buildMatomoHits([][2]string{
				{"page_view", "https://example.com/page1"},
				{"outlink", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"download", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
				{"outlink", "https://example.com/page3"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(0), int64(0), int64(0), int64(1), int64(0)},
			description: "Only last page_view should be marked as exit",
		},
		{
			name: "SSEIsEntryPage - consecutive page_views",
			hits: buildMatomoHits([][2]string{
				{"page_view", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(1), int64(0), int64(0)},
			description: "Only first of consecutive page_views should be marked as entry",
		},
		{
			name: "SSEIsExitPage - consecutive page_views",
			hits: buildMatomoHits([][2]string{
				{"page_view", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(0), int64(1)},
			description: "Only last of consecutive page_views should be marked as exit",
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
					writeResult := make([]any, 0, len(whd.WriteCalls[0].Records))
					for _, record := range whd.WriteCalls[0].Records {
						writeResult = append(writeResult, record[tc.field])
					}
					assert.Equal(t, tc.expected, writeResult, tc.description)
				},
				proto,
			)
		})
	}
}

func TestMatomoSSETimeOnPage(t *testing.T) {
	proto := NewMatomoProtocol(&staticPropertyIDExtractor{propertyID: "test_property_id"})

	var testCases = []struct {
		name        string
		hits        columntests.TestHits
		expected    []any
		description string
	}{
		{
			name: "Session starting with page_view - basic example",
			hits: buildMatomoHitsWithTime([][3]any{
				{"page_view", "https://example.com/page1", 0},
				{"outlink", "https://example.com/page1", 1},
				{"outlink", "https://example.com/page1", 2},
				{"page_view", "https://example.com/page2", 10},
				{"outlink", "https://example.com/page2", 12},
			}),
			expected: []any{int64(10), int64(10), int64(10), int64(2), int64(2)},
			description: "First page_view at 0s, next at 10s (10s time_on_page), " +
				"second page_view at 10s, last event at 12s (2s time_on_page)",
		},
		{
			name: "Session not starting with page_view - treat first event as page_view",
			hits: buildMatomoHitsWithTime([][3]any{
				{"outlink", "https://example.com/page1", 0},
				{"download", "https://example.com/page1", 1},
				{"page_view", "https://example.com/page2", 5},
				{"outlink", "https://example.com/page2", 6},
			}),
			expected: []any{int64(5), int64(5), int64(1), int64(1)},
			description: "First event (outlink) treated as page boundary at 0s, next page_view at 5s (5s), " +
				"second page at 5s, last event at 6s (1s time_on_page)",
		},
		{
			name: "Session ending with page_view - time_on_page is 0",
			hits: buildMatomoHitsWithTime([][3]any{
				{"page_view", "https://example.com/page1", 0},
				{"outlink", "https://example.com/page1", 1},
				{"page_view", "https://example.com/page2", 10},
			}),
			expected: []any{int64(10), int64(10), int64(0)},
			description: "First page_view at 0s, next at 10s (10s), " +
				"second page_view at 10s with no subsequent page_view, so time_on_page = 0",
		},
		{
			name: "Session not ending with page_view - use last event time",
			hits: buildMatomoHitsWithTime([][3]any{
				{"page_view", "https://example.com/page1", 0},
				{"outlink", "https://example.com/page1", 1},
				{"page_view", "https://example.com/page2", 10},
				{"outlink", "https://example.com/page2", 12},
				{"download", "https://example.com/page2", 15},
			}),
			expected: []any{int64(10), int64(10), int64(5), int64(5), int64(5)},
			description: "First page_view at 0s, next at 10s (10s), " +
				"second page_view at 10s, last event at 15s (5s time_on_page)",
		},
		{
			name: "Session with single page_view event",
			hits: buildMatomoHitsWithTime([][3]any{
				{"page_view", "https://example.com/page1", 0},
			}),
			expected:    []any{int64(0)},
			description: "Single page_view with no subsequent page_view, time_on_page = 0",
		},
		{
			name: "Session with no page_views at all",
			hits: buildMatomoHitsWithTime([][3]any{
				{"outlink", "https://example.com/page1", 0},
				{"download", "https://example.com/page1", 2},
				{"site_search", "https://example.com/page1", 5},
			}),
			expected:    []any{int64(5), int64(5), int64(5)},
			description: "No page_views, first event treated as page boundary at 0s, last event at 5s (5s for all)",
		},
		{
			name: "Multiple page_views with various event types",
			hits: buildMatomoHitsWithTime([][3]any{
				{"page_view", "https://example.com/page1", 0},
				{"outlink", "https://example.com/page1", 2},
				{"download", "https://example.com/page1", 4},
				{"page_view", "https://example.com/page2", 10},
				{"site_search", "https://example.com/page2", 11},
				{"page_view", "https://example.com/page3", 15},
				{"outlink", "https://example.com/page3", 16},
				{"download", "https://example.com/page3", 18},
			}),
			expected:    []any{int64(10), int64(10), int64(10), int64(5), int64(5), int64(3), int64(3), int64(3)},
			description: "First page: 0-10s (10s), second page: 10-15s (5s), third page: 15-18s (3s)",
		},
		{
			name: "Consecutive page_views",
			hits: buildMatomoHitsWithTime([][3]any{
				{"page_view", "https://example.com/page1", 0},
				{"page_view", "https://example.com/page2", 5},
				{"page_view", "https://example.com/page3", 10},
			}),
			expected:    []any{int64(5), int64(5), int64(0)},
			description: "First page_view at 0s, next at 5s (5s), second at 5s next at 10s (5s), third at 10s (0s)",
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
					writeResult := make([]any, 0, len(whd.WriteCalls[0].Records))
					for _, record := range whd.WriteCalls[0].Records {
						writeResult = append(writeResult, record["time_on_page"])
					}
					assert.Equal(t, tc.expected, writeResult, tc.description)
				},
				proto,
			)
		})
	}
}
