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
		name               string
		method             string
		queryParams        url.Values
		body               string
		expectedHits       int
		expectedEvent      string
		expectedEventNames []string
		expectedClientID   *hits.ClientID
		expectedUserID     *string
		expectNilUserID    bool
		expectError        bool
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
			expectedEvent: "custom_event",
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
			name: "single_post_site_search_when_search_empty",
			queryParams: url.Values{
				"idsite": []string{"12"},
				"search": []string{""},
				"e_c":    []string{"Videos"},
				"e_a":    []string{"Play"},
				"_id":    []string{"abc133"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "site_search",
		},
		{
			name: "single_post_content_impression",
			queryParams: url.Values{
				"idsite": []string{"13"},
				"c_n":    []string{"Hero Banner"},
				"_id":    []string{"abc134"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "content_impression",
		},
		{
			name: "single_post_content_interaction",
			queryParams: url.Values{
				"idsite": []string{"14"},
				"c_i":    []string{"click"},
				"c_n":    []string{"Hero Banner"},
				"_id":    []string{"abc135"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "content_interaction",
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
			name:         "bulk_json_post_two_requests",
			queryParams:  nil,
			method:       "POST",
			body:         `{"requests":["?idsite=6&_id=abc128","?idsite=7&_id=abc129"]}`,
			expectedHits: 2,
			expectedEventNames: []string{
				"page_view",
				"page_view",
			},
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
		{
			name: "user_id_from_uid",
			queryParams: url.Values{
				"idsite": []string{"9"},
				"uid":    []string{"user@example.com"},
				"_id":    []string{"abc131"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "page_view",
			expectedUserID: func() *string {
				value := "user@example.com"
				return &value
			}(),
		},
		{
			name: "missing_uid_is_nil",
			queryParams: url.Values{
				"idsite": []string{"10"},
				"_id":    []string{"abc132"},
			},
			method:          "POST",
			body:            "",
			expectedHits:    1,
			expectedEvent:   "page_view",
			expectNilUserID: true,
		},
		{
			name: "client_id_from_cid",
			queryParams: url.Values{
				"idsite": []string{"11"},
				"cid":    []string{"abcd1234abcd5678"},
			},
			method:        "POST",
			body:          "",
			expectedHits:  1,
			expectedEvent: "page_view",
			expectedClientID: func() *hits.ClientID {
				value := hits.ClientID("abcd1234abcd5678")
				return &value
			}(),
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
				for _, hit := range hitsResult {
					assert.NotEmpty(t, hit.ClientID)
				}
				if len(tc.expectedEventNames) > 0 {
					require.Len(t, hitsResult, len(tc.expectedEventNames))
					for index, expectedEvent := range tc.expectedEventNames {
						assert.Equal(t, expectedEvent, hitsResult[index].EventName)
					}
				}
				if tc.expectedEvent != "" {
					assert.Equal(t, tc.expectedEvent, hitsResult[0].EventName)
				}
				if tc.expectNilUserID {
					assert.Nil(t, hitsResult[0].UserID)
				} else if tc.expectedUserID != nil {
					require.NotNil(t, hitsResult[0].UserID)
					assert.Equal(t, *tc.expectedUserID, *hitsResult[0].UserID)
				}
				if tc.expectedClientID != nil {
					assert.Equal(t, *tc.expectedClientID, hitsResult[0].ClientID)
				}
			}
		})
	}
}

func TestEndpoints(t *testing.T) {
	t.Run("default endpoint only", func(t *testing.T) {
		proto := NewMatomoProtocol(&testPropertyIDExtractor{})

		assert.Equal(t, []protocol.ProtocolEndpoint{
			{
				Methods: []string{fasthttp.MethodPost, fasthttp.MethodGet},
				Path:    "/matomo.php",
			},
		}, proto.Endpoints())
	})

	t.Run("adds normalized extra endpoints", func(t *testing.T) {
		proto := NewMatomoProtocol(
			&testPropertyIDExtractor{},
			WithExtraTrackingEndpoints([]string{"piwik.php", "/track", " ", "/matomo.php", "track"}),
		)

		assert.Equal(t, []protocol.ProtocolEndpoint{
			{
				Methods: []string{fasthttp.MethodPost, fasthttp.MethodGet},
				Path:    "/matomo.php",
			},
			{
				Methods: []string{fasthttp.MethodPost, fasthttp.MethodGet},
				Path:    "/piwik.php",
			},
			{
				Methods: []string{fasthttp.MethodPost, fasthttp.MethodGet},
				Path:    "/track",
			},
		}, proto.Endpoints())
	})
}
