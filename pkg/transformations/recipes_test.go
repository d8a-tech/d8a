package transformations

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These are the single-rule recipes behind the cloud editor templates and the docs.

func applyToPage(t *testing.T, p Program, location string) *hits.Hit {
	t.Helper()
	hit := hits.New()
	hit.Request.QueryParams.Set("dl", location)
	keep, err := apply(p, "ga4", hit, nil, columnDeps(nil, nil))
	require.NoError(t, err)
	require.True(t, keep)
	return hit
}

func TestRecipeLanguagePrefix(t *testing.T) {
	// given
	p := compileRules(t, rule{
		Expression: `page_path != nil && page_path matches "^/[a-z]{2}(/|$)"`,
		Actions:    []action{replace(field("page_path"), `^/[a-z]{2}(/|$)`, text("/"))},
	})
	for location, want := range map[string]string{
		"https://example.com/de/pricing?x=1": "https://example.com/pricing?x=1",
		"https://example.com/de":             "https://example.com/",
		"https://example.com/design/x":       "https://example.com/design/x",
		"https://example.com/pricing":        "https://example.com/pricing",
	} {
		// when
		hit := applyToPage(t, p, location)
		// then
		assert.Equal(t, want, hit.Request.QueryParams.Get("dl"), location)
	}
}

func TestRecipeURLParameterToEventParameter(t *testing.T) {
	// given
	p := compileRules(t, rule{
		Expression: `url_params["q"] != nil`,
		Actions: []action{
			template(parameter("search_term", "string"), "{{url_params.q}}"),
			remove(urlParameter("q")),
		},
	})
	// when
	hit := applyToPage(t, p, "https://example.com/search?q=shoes&page=2")
	// then
	assert.Equal(t, "shoes", hit.Request.QueryParams.Get("ep.search_term"))
	assert.Equal(t, "https://example.com/search?page=2", hit.Request.QueryParams.Get("dl"))
	// when: no parameter, nothing changes
	hit = applyToPage(t, p, "https://example.com/search?page=2")
	// then
	assert.False(t, hit.Request.QueryParams.Has("ep.search_term"))
}

func TestRecipeTrailingSlashAndWWW(t *testing.T) {
	// given
	p := compileRules(t,
		rule{Expression: "true", Actions: []action{replace(field("page_path"), `(.)/$`, text("$1"))}},
		rule{Expression: "true", Actions: []action{replace(field("page_hostname"), `^www\.`, text(""))}},
	)
	for location, want := range map[string]string{
		"https://www.shop.com/pricing/?a=1": "https://shop.com/pricing?a=1",
		"https://www.shop.com/":             "https://shop.com/",
		"https://shop.com/a/b":              "https://shop.com/a/b",
	} {
		// when
		hit := applyToPage(t, p, location)
		// then
		assert.Equal(t, want, hit.Request.QueryParams.Get("dl"), location)
	}
}

func TestRecipeOwnDomainsOnly(t *testing.T) {
	// given
	p := compileRules(t, rule{
		Expression: `page_hostname != nil && page_hostname not in ["example.com", "www.example.com"]`,
		Actions:    []action{{Type: "drop"}},
	})
	for location, wantKeep := range map[string]bool{
		"https://www.example.com/": true, "https://copy.example.net/": false,
	} {
		hit := hits.New()
		hit.Request.QueryParams.Set("dl", location)
		// when
		keep, err := apply(p, "ga4", hit, nil, columnDeps(nil, nil))
		// then
		require.NoError(t, err)
		assert.Equal(t, wantKeep, keep, location)
	}
	// when: events without a page are kept
	keep, err := apply(p, "ga4", hits.New(), nil, columnDeps(nil, nil))
	// then
	require.NoError(t, err)
	assert.True(t, keep)
}
