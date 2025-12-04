package ga4

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func simpleHits(h [][3]string) columntests.TestHits {
	var allHits = make([]*hits.Hit, len(h))
	for i, hit := range h {
		allHits[i] = hits.New()
		allHits[i].QueryParams.Add("en", hit[0])
		allHits[i].QueryParams.Add("dl", hit[1])
		allHits[i].QueryParams.Add("dt", hit[2])
	}
	return allHits
}

// nolint:funlen // test code
func TestValueTransitions(t *testing.T) {
	var testCases = []struct {
		name     string
		hits     columntests.TestHits
		field    string
		expected []any
	}{
		{
			name: "PreviousPageLocation",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
				{"page_view", "https://example.com/page2", "Page 2"},
				{"scroll", "https://example.com/page2", "Page 2"},
				{"scroll", "https://example.com/page2", "Page 2"},
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
		},
		{
			name: "PreviousPageTitle",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1", "Page 1"},
				{"page_view", "https://example.com/page2", "Page 2"},
				{"scroll", "https://example.com/page2", "Page 2"},
				{"scroll", "https://example.com/page2", "Page 2"},
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
		},
		{
			name: "NextPageLocation",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
				{"page_view", "https://example.com/page2", "Page 2"},
				{"scroll", "https://example.com/page2", "Page 2"},
				{"scroll", "https://example.com/page2", "Page 2"},
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
		},
		{
			name: "NextPageTitle",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1", "Page 1"},
				{"page_view", "https://example.com/page2", "Page 2"},
				{"scroll", "https://example.com/page2", "Page 2"},
				{"scroll", "https://example.com/page2", "Page 2"},
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
						writeResult = append(writeResult, record[tc.field])
					}
					assert.Equal(t, tc.expected, writeResult)
				},
				NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
			)
		})
	}
}

// nolint:funlen // test code
func TestIsEntryExitPage(t *testing.T) {
	var testCases = []struct {
		name        string
		hits        columntests.TestHits
		field       string
		expected    []any
		description string
	}{
		{
			name: "SSEIsEntryPage - no page_view in session",
			hits: simpleHits([][3]string{
				{"scroll", "https://example.com/page1"},
				{"click", "https://example.com/page1"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(0), int64(0)},
			description: "No page_view events, all should be 0",
		},
		{
			name: "SSEIsExitPage - no page_view in session",
			hits: simpleHits([][3]string{
				{"scroll", "https://example.com/page1"},
				{"click", "https://example.com/page1"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(0)},
			description: "No page_view events, all should be 0",
		},
		{
			name: "SSEIsEntryPage - single page_view",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(1)},
			description: "Single page_view should be marked as entry",
		},
		{
			name: "SSEIsExitPage - single page_view",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(1)},
			description: "Single page_view should be marked as exit",
		},
		{
			name: "SSEIsEntryPage - session starting with page_view",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
				{"scroll", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"click", "https://example.com/page2"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(1), int64(0), int64(0), int64(0)},
			description: "First page_view should be marked as entry",
		},
		{
			name: "SSEIsExitPage - session ending with page_view",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
				{"scroll", "https://example.com/page1"},
				{"click", "https://example.com/page2"},
				{"page_view", "https://example.com/page2"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(0), int64(0), int64(1)},
			description: "Last page_view should be marked as exit",
		},
		{
			name: "SSEIsEntryPage - session not starting with page_view",
			hits: simpleHits([][3]string{
				{"scroll", "https://example.com/page1"},
				{"click", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(0), int64(0), int64(1), int64(0)},
			description: "First page_view (not first event) should be marked as entry",
		},
		{
			name: "SSEIsExitPage - session not ending with page_view",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"scroll", "https://example.com/page2"},
				{"click", "https://example.com/page2"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(1), int64(0), int64(0)},
			description: "Last page_view (not last event) should be marked as exit",
		},
		{
			name: "SSEIsEntryPage - multiple page_views with non-pv events",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
				{"scroll", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"click", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
				{"scroll", "https://example.com/page3"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(1), int64(0), int64(0), int64(0), int64(0), int64(0)},
			description: "Only first page_view should be marked as entry",
		},
		{
			name: "SSEIsExitPage - multiple page_views with non-pv events",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
				{"scroll", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"click", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
				{"scroll", "https://example.com/page3"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(0), int64(0), int64(0), int64(1), int64(0)},
			description: "Only last page_view should be marked as exit",
		},
		{
			name: "SSEIsEntryPage - consecutive page_views",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(1), int64(0), int64(0)},
			description: "Only first of consecutive page_views should be marked",
		},
		{
			name: "SSEIsExitPage - consecutive page_views",
			hits: simpleHits([][3]string{
				{"page_view", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(0), int64(1)},
			description: "Only last of consecutive page_views should be marked",
		},
		{
			name: "SSEIsEntryPage - session starting and ending with non-pv",
			hits: simpleHits([][3]string{
				{"scroll", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
				{"click", "https://example.com/page3"},
			}),
			field:       "session_is_entry_page",
			expected:    []any{int64(0), int64(1), int64(0), int64(0)},
			description: "First page_view should be marked even if not first event",
		},
		{
			name: "SSEIsExitPage - session starting and ending with non-pv",
			hits: simpleHits([][3]string{
				{"scroll", "https://example.com/page1"},
				{"page_view", "https://example.com/page2"},
				{"page_view", "https://example.com/page3"},
				{"click", "https://example.com/page3"},
			}),
			field:       "session_is_exit_page",
			expected:    []any{int64(0), int64(0), int64(1), int64(0)},
			description: "Last page_view should be marked even if not last event",
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
						writeResult = append(writeResult, record[tc.field])
					}
					assert.Equal(t, tc.expected, writeResult, tc.description)
				},
				NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry()),
			)
		})
	}
}
