package columntests

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPageViewTestHit(eventName string, offsetSeconds int, pageURL, pageTitle string) *hits.Hit {
	h := hits.New()
	h.EventName = eventName
	h.MustParsedRequest().QueryParams.Set("v", "2")
	h.MustParsedRequest().QueryParams.Set("tid", "G-TESTMEASUREMENTID")
	h.MustParsedRequest().QueryParams.Set("en", eventName)
	h.MustParsedRequest().QueryParams.Set("dl", pageURL)
	h.MustParsedRequest().QueryParams.Set("dt", pageTitle)
	h.MustParsedRequest().ServerReceivedTime = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC).
		Add(time.Duration(offsetSeconds) * time.Second)
	return h
}

func TestPageViewSSECoreColumns(t *testing.T) {
	proto := ga4.NewGA4Protocol(currency.NewDummyConverter(1), properties.NewTestSettingRegistry())

	t.Run("transitions", func(t *testing.T) {
		testCases := []struct {
			name     string
			hits     TestHits
			field    string
			expected []any
		}{
			{
				name: "previous_page_location with repeated same-page non-page-view events",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 1, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 2, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 3, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("page_view", 4, "https://example.com/page3", "Page 3"),
				},
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
				name: "next_page_location with repeated same-page non-page-view events",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 1, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 2, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 3, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("page_view", 4, "https://example.com/page3", "Page 3"),
				},
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
				name: "previous_page_title with repeated same-page non-page-view events",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 1, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 2, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 3, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("page_view", 4, "https://example.com/page3", "Page 3"),
				},
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
				name: "next_page_title with repeated same-page non-page-view events",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 1, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 2, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 3, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("page_view", 4, "https://example.com/page3", "Page 3"),
				},
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
				ColumnTestCase(t, tc.hits, func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					require.NoError(t, closeErr)
					result := make([]any, 0, len(whd.WriteCalls[0].Records))
					for _, record := range whd.WriteCalls[0].Records {
						result = append(result, record[tc.field])
					}
					assert.Equal(t, tc.expected, result)
				}, proto)
			})
		}
	})

	t.Run("entry and exit", func(t *testing.T) {
		testCases := []struct {
			name          string
			hits          TestHits
			entryExpected []any
			exitExpected  []any
		}{
			{
				name: "no page views in session",
				hits: TestHits{
					newPageViewTestHit("scroll", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("click", 1, "https://example.com/page1", "Page 1"),
				},
				entryExpected: []any{false, false},
				exitExpected:  []any{false, false},
			},
			{
				name: "single page view session",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
				},
				entryExpected: []any{true},
				exitExpected:  []any{true},
			},
			{
				name: "mixed with non page view events",
				hits: TestHits{
					newPageViewTestHit("scroll", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 1, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("click", 2, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("page_view", 3, "https://example.com/page3", "Page 3"),
					newPageViewTestHit("download", 4, "https://example.com/page3", "Page 3"),
				},
				entryExpected: []any{false, true, false, false, false},
				exitExpected:  []any{false, false, false, true, false},
			},
			{
				name: "consecutive page views",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 1, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("page_view", 2, "https://example.com/page3", "Page 3"),
				},
				entryExpected: []any{true, false, false},
				exitExpected:  []any{false, false, true},
			},
			{
				name: "session starts and ends with non page view events",
				hits: TestHits{
					newPageViewTestHit("scroll", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 1, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("page_view", 2, "https://example.com/page3", "Page 3"),
					newPageViewTestHit("click", 3, "https://example.com/page3", "Page 3"),
				},
				entryExpected: []any{false, true, false, false},
				exitExpected:  []any{false, false, true, false},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ColumnTestCase(t, tc.hits, func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					require.NoError(t, closeErr)
					entry := make([]any, 0, len(whd.WriteCalls[0].Records))
					exit := make([]any, 0, len(whd.WriteCalls[0].Records))
					for _, record := range whd.WriteCalls[0].Records {
						entry = append(entry, record["session_is_entry_page"])
						exit = append(exit, record["session_is_exit_page"])
					}
					assert.Equal(t, tc.entryExpected, entry)
					assert.Equal(t, tc.exitExpected, exit)
				}, proto)
			})
		}
	})

	t.Run("time on page", func(t *testing.T) {
		testCases := []struct {
			name     string
			hits     TestHits
			expected []any
		}{
			{
				name: "session starts without page view",
				hits: TestHits{
					newPageViewTestHit("scroll", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("click", 1, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 5, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 6, "https://example.com/page2", "Page 2"),
				},
				expected: []any{int64(5), int64(5), int64(1), int64(1)},
			},
			{
				name: "session ends with page view",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("scroll", 1, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 10, "https://example.com/page2", "Page 2"),
				},
				expected: []any{int64(10), int64(10), int64(0)},
			},
			{
				name: "session with no page views",
				hits: TestHits{
					newPageViewTestHit("scroll", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("click", 2, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("download", 5, "https://example.com/page1", "Page 1"),
				},
				expected: []any{int64(5), int64(5), int64(5)},
			},
			{
				name: "single page view",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
				},
				expected: []any{int64(0)},
			},
			{
				name: "multiple page views with mixed events",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("scroll", 1, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 10, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("click", 12, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("page_view", 15, "https://example.com/page3", "Page 3"),
					newPageViewTestHit("download", 18, "https://example.com/page3", "Page 3"),
				},
				expected: []any{int64(10), int64(10), int64(5), int64(5), int64(3), int64(3)},
			},
			{
				name: "consecutive page views",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 5, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("page_view", 10, "https://example.com/page3", "Page 3"),
				},
				expected: []any{int64(5), int64(5), int64(0)},
			},
			{
				name: "page view followed immediately by another page view",
				hits: TestHits{
					newPageViewTestHit("page_view", 0, "https://example.com/page1", "Page 1"),
					newPageViewTestHit("page_view", 0, "https://example.com/page2", "Page 2"),
					newPageViewTestHit("scroll", 1, "https://example.com/page2", "Page 2"),
				},
				expected: []any{int64(0), int64(1), int64(1)},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ColumnTestCase(t, tc.hits, func(t *testing.T, closeErr error, whd *warehouse.MockWarehouseDriver) {
					require.NoError(t, closeErr)
					result := make([]any, 0, len(whd.WriteCalls[0].Records))
					for _, record := range whd.WriteCalls[0].Records {
						result = append(result, record["time_on_page"])
					}
					assert.Equal(t, tc.expected, result)
				}, proto)
			})
		}
	})
}
