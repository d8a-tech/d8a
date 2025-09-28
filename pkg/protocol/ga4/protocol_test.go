package ga4

import (
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHits(t *testing.T) {
	// given
	testCases := []struct {
		name           string
		queryParams    url.Values
		body           string
		expectedHits   int
		expectedParams []map[string]string // slice of param maps, one per hit
	}{
		{
			name: "only_query_params",
			queryParams: url.Values{
				"v":   []string{"2"},
				"tid": []string{"G-2VEWJC5YPE"},
				"cid": []string{"1369988567.1756151735"},
				"_p":  []string{"1758117262938"},
				"en":  []string{"page_view"},
			},
			body:         "",
			expectedHits: 1,
			expectedParams: []map[string]string{
				{
					"v":   "2",
					"tid": "G-2VEWJC5YPE",
					"cid": "1369988567.1756151735",
					"_p":  "1758117262938",
					"en":  "page_view",
				},
			},
		},
		{
			name: "query_params_plus_one_line",
			queryParams: url.Values{
				"v":   []string{"2"},
				"tid": []string{"G-2VEWJC5YPE"},
				"cid": []string{"1369988567.1756151735"},
				"_p":  []string{"1758117262938"},
			},
			body:         "en=page_view&_et=3674",
			expectedHits: 1,
			expectedParams: []map[string]string{
				{
					"v":   "2",
					"tid": "G-2VEWJC5YPE",
					"cid": "1369988567.1756151735",
					"_p":  "1758117262938",
					"en":  "page_view",
					"_et": "3674",
				},
			},
		},
		{
			name: "query_params_plus_two_lines_with_override",
			queryParams: url.Values{
				"v":   []string{"2"},
				"tid": []string{"G-2VEWJC5YPE"},
				"cid": []string{"1369988567.1756151735"},
				"_p":  []string{"1758117262938"},
				"en":  []string{"page_view"},
			},
			body:         "en=page_view&_et=3674\nen=click&_et=799&dt=Test%20Page",
			expectedHits: 2,
			expectedParams: []map[string]string{
				{
					"v":   "2",
					"tid": "G-2VEWJC5YPE",
					"cid": "1369988567.1756151735",
					"_p":  "1758117262938",
					"en":  "page_view",
					"_et": "3674",
				},
				{
					"v":   "2",
					"tid": "G-2VEWJC5YPE",
					"cid": "1369988567.1756151735",
					"_p":  "1758117262938",
					"en":  "click",
					"_et": "799",
					"dt":  "Test Page",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			ga4Protocol := NewGA4Protocol()
			request := &protocol.Request{
				QueryParams: tc.queryParams,
				Headers:     map[string][]string{},
				Host:        []byte("example.com"),
				Path:        []byte("/g/collect"),
				Method:      []byte("POST"),
				Body:        io.NopCloser(strings.NewReader(tc.body)),
			}

			// when
			hits, err := ga4Protocol.Hits(request)

			// then
			require.NoError(t, err)
			assert.Len(t, hits, tc.expectedHits)

			// Check parameters for each hit
			for i, expectedParams := range tc.expectedParams {
				hit := hits[i]
				for param, expectedValue := range expectedParams {
					actualValue := hit.QueryParams.Get(param)
					assert.Equal(t, expectedValue, actualValue, "Hit %d: Parameter %s should match", i, param)
				}
			}
		})
	}
}
