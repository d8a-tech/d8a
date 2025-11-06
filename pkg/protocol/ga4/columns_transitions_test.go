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
				NewGA4Protocol(currency.NewDummyConverter(1), NewStaticPropertySource([]properties.PropertyConfig{
					{
						PropertyID:         "1234567890",
						PropertyName:       "Test Property",
						PropertyTrackingID: "G-2VEWJC5YPE",
					},
				})),
			)
		})
	}
}
