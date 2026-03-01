package matomo

import (
	"errors"
	"net/url"
	"testing"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

type testPropertyIDExtractor struct{}

func (e *testPropertyIDExtractor) PropertyID(ctx *protocol.RequestContext) (string, error) {
	propertyID := ctx.Parsed.QueryParams.Get("idsite")
	if propertyID == "" {
		return "", errors.New("missing idsite")
	}
	return propertyID, nil
}

func TestHits(t *testing.T) {
	// given
	testCases := []struct {
		name          string
		method        string
		queryParams   url.Values
		body          string
		expectedHits  int
		expectedEvent string
		expectError   bool
	}{
		{
			name: "single_get_default_page_view",
			queryParams: url.Values{
				"idsite": []string{"1"},
				"_id":    []string{"abc123"},
			},
			method:        "GET",
			body:          "",
			expectedHits:  1,
			expectedEvent: "page_view",
		},
		{
			name: "single_post_event",
			queryParams: url.Values{
				"idsite": []string{"2"},
				"e_c":    []string{"Videos"},
				"e_a":    []string{"Play"},
				"_id":    []string{"abc124"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "event",
		},
		{
			name: "single_post_site_search",
			queryParams: url.Values{
				"idsite": []string{"3"},
				"search": []string{"golang"},
				"_id":    []string{"abc125"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "site_search",
		},
		{
			name: "single_post_download",
			queryParams: url.Values{
				"idsite":   []string{"4"},
				"download": []string{"https://example.com/file.zip"},
				"_id":      []string{"abc126"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "download",
		},
		{
			name: "single_post_outlink",
			queryParams: url.Values{
				"idsite": []string{"5"},
				"link":   []string{"https://example.com"},
				"_id":    []string{"abc127"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "outlink",
		},
		{
			name: "bulk_json_post_two_requests",
			queryParams: url.Values{
				"_id": []string{"abc128"},
			},
			method:       "POST",
			body:         `{"requests":["?idsite=6&_id=abc128","?idsite=7&_id=abc129"]}`,
			expectedHits: 2,
		},
		{
			name: "missing_client_id_generates",
			queryParams: url.Values{
				"idsite": []string{"8"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "page_view",
		},
		{
			name: "missing_idsite_returns_error",
			queryParams: url.Values{
				"_id": []string{"abc130"},
			},
			method:      "POST",
			body:        "",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			matomoProtocol := NewMatomoProtocol(&testPropertyIDExtractor{})
			request := &hits.ParsedRequest{
				QueryParams: tc.queryParams,
				Headers:     map[string][]string{},
				Host:        "example.com",
				Path:        "/matomo.php",
				Method:      tc.method,
				Body:        []byte(tc.body),
			}

			// when
			hitsResult, err := matomoProtocol.Hits(&fasthttp.RequestCtx{}, request)

			// then
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Len(t, hitsResult, tc.expectedHits)

			if tc.expectedHits > 0 {
				if tc.expectedEvent != "" {
					assert.Equal(t, tc.expectedEvent, hitsResult[0].EventName)
				}
				assert.NotEmpty(t, hitsResult[0].ClientID)
			}
		})
	}
}
