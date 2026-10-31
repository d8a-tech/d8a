package hits

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsedRequestCloneQueryParams(t *testing.T) {
	cases := []struct {
		name   string
		params url.Values
	}{
		{name: "nil params", params: nil},
		{name: "empty params", params: url.Values{}},
		{name: "key without values", params: url.Values{"empty": {}}},
		{name: "single values", params: url.Values{"v": {"2"}, "tid": {"G-1"}, "en": {"page_view"}}},
		{name: "multi-value keys", params: url.Values{"a": {"1", "2"}, "b": {"3"}, "c": {"4", "5", "6"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			source := &ParsedRequest{QueryParams: tc.params, Headers: http.Header{"X-Test": {"1"}}}

			// when
			clone := source.Clone()

			// then
			assert.NotNil(t, clone.QueryParams)
			assert.Len(t, clone.QueryParams, len(tc.params))
			for key, values := range tc.params {
				assert.Equal(t, values, clone.QueryParams[key], key)
			}
			assert.Equal(t, source.Headers, clone.Headers)
		})
	}
}

func TestParsedRequestCloneIsIndependent(t *testing.T) {
	cases := []struct {
		name          string
		mutate        func(url.Values)
		expectedClone url.Values
	}{
		{
			name:          "overwrite value",
			mutate:        func(v url.Values) { v["a"][1] = "changed" },
			expectedClone: url.Values{"a": {"1", "changed"}, "b": {"3"}, "c": {"4", "5"}},
		},
		{
			name:          "append to key does not overwrite the next key",
			mutate:        func(v url.Values) { v["a"] = append(v["a"], "x"); v["b"] = append(v["b"], "y") },
			expectedClone: url.Values{"a": {"1", "2", "x"}, "b": {"3", "y"}, "c": {"4", "5"}},
		},
		{
			name:          "add and set",
			mutate:        func(v url.Values) { v.Add("c", "6"); v.Set("b", "set") },
			expectedClone: url.Values{"a": {"1", "2"}, "b": {"set"}, "c": {"4", "5", "6"}},
		},
		{
			name:          "delete key",
			mutate:        func(v url.Values) { v.Del("a") },
			expectedClone: url.Values{"b": {"3"}, "c": {"4", "5"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			source := &ParsedRequest{QueryParams: url.Values{"a": {"1", "2"}, "b": {"3"}, "c": {"4", "5"}}}
			clone := source.Clone()

			// when
			tc.mutate(clone.QueryParams)

			// then
			assert.Equal(t, url.Values{"a": {"1", "2"}, "b": {"3"}, "c": {"4", "5"}}, source.QueryParams)
			assert.Equal(t, tc.expectedClone, clone.QueryParams)
		})
	}
}
