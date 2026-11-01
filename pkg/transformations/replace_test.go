package transformations

import (
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Patterns offered by the cloud editor's pattern library; they must work with RE2.
const (
	emailPattern = `(?i:[a-z0-9._%+-]+(?:@|%40)[a-z0-9.-]+\.[a-z]{2,})`
	phonePattern = `(?:\+|%2B)\d(?:[ .()-]?\d){7,14}`
	cardPattern  = `\d(?:[ -]?\d){12,18}`
	ipPattern    = `\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}`
	idsPattern   = `(/)(?:\d+|(?i:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}))(/|\?|#|$)`
	piiPattern   = emailPattern + "|" + phonePattern + "|" + cardPattern + "|" + ipPattern
)

func replace(target *ref, pattern string, replacement *string) action {
	return action{Type: "replace", Target: target, Pattern: pattern, Replacement: replacement}
}

func text(value string) *string { return &value }

func TestLibraryPatterns(t *testing.T) {
	for _, tc := range []struct {
		name, pattern, replacement, input, want string
	}{
		{"email", emailPattern, "[REDACTED]", "Contact jan.kowalski@example.com now", "Contact [REDACTED] now"},
		{"encoded email", emailPattern, "[REDACTED]",
			"/thanks?email=jan.kowalski%40example.com&x=1", "/thanks?email=[REDACTED]&x=1"},
		{"phone", phonePattern, "[REDACTED]", "Call +48 600 123 456 today", "Call [REDACTED] today"},
		{"phone without plus untouched", phonePattern, "[REDACTED]", "order 600123456", "order 600123456"},
		{"card", cardPattern, "[REDACTED]", "card 4111 1111 1111 1111 ok", "card [REDACTED] ok"},
		{"ipv4", ipPattern, "[REDACTED]", "from 203.0.113.42 via", "from [REDACTED] via"},
		{"short numbers untouched", ipPattern + "|" + cardPattern, "[REDACTED]", "order 12345 v1.2", "order 12345 v1.2"},
		{"numeric id", idsPattern, "$1:id$2",
			"https://shop.example.com/orders/12345/items?x=1", "https://shop.example.com/orders/:id/items?x=1"},
		{"uuid at end", idsPattern, "$1:id$2", "/users/3F2504E0-4F89-11D3-9A0C-0305E82C3301", "/users/:id"},
		{"combined", piiPattern, "[REDACTED]", "a@b.co, +4860012345, 10.0.0.1", "[REDACTED], [REDACTED], [REDACTED]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// when
			got := regexp.MustCompile(tc.pattern).ReplaceAllString(tc.input, tc.replacement)
			// then
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestReplaceAction(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.QueryParams.Set("dl", "https://shop.example.com/orders/123?email=jan%40example.com&secret=keep")
	hit.Request.QueryParams.Set("dt", "Order for jan@example.com | Example Shop")
	hit.Request.QueryParams.Set("ep.search_term", "call +48 600 123 456")
	hit.Request.QueryParams.Set("dr", "https://www.google.com/search?q=private")
	settings := &properties.Settings{ExcludedURLParams: []string{"secret"}}
	p := compileRules(t,
		rule{Expression: "true", Actions: []action{
			replace(field("page_title"), emailPattern, nil),
			replace(parameter("search_term", "string"), phonePattern, text("")),
			replace(field("page_referrer"), `\?.*$`, text("")),
		}},
		rule{Expression: "true", Actions: []action{
			replace(field("page_location"), piiPattern, nil),
		}},
		rule{Expression: "true", Actions: []action{
			replace(field("page_location"), idsPattern, text("$1:id$2")),
			replace(field("page_title"), `\s*[|–-]\s*Example Shop$`, text("")),
		}},
	)
	// when
	keep, err := apply(p, "ga4", hit, settings, columnDeps(settings, nil))
	// then
	require.NoError(t, err)
	assert.True(t, keep)
	location := hit.Request.QueryParams.Get("dl")
	assert.Equal(t, "https://shop.example.com/orders/:id?email=[REDACTED]&secret=keep", location)
	_, parseErr := url.Parse(location)
	assert.NoError(t, parseErr, "redacted URLs stay parseable for page columns")
	assert.Equal(t, "Order for [REDACTED]", hit.Request.QueryParams.Get("dt"))
	assert.Equal(t, "call ", hit.Request.QueryParams.Get("ep.search_term"))
	assert.Equal(t, "https://www.google.com/search", hit.Request.QueryParams.Get("dr"))
}

func TestReplaceSkipsMissingUnchangedAndOversizedValues(t *testing.T) {
	// given
	long := strings.Repeat("a@b.co ", MaxReplaceInputLength/7+1)
	hit := hits.New()
	hit.Request.QueryParams.Set("dt", "nothing to see")
	hit.Request.QueryParams.Set("ep.long", long)
	p := compileRules(t, rule{Expression: "true", Actions: []action{
		replace(field("page_title"), emailPattern, nil),
		replace(field("page_referrer"), emailPattern, nil),
		replace(parameter("long", "string"), emailPattern, nil),
	}})
	// when
	keep, err := apply(p, "ga4", hit, nil, ExtractionDependencies{})
	// then
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Equal(t, "nothing to see", hit.Request.QueryParams.Get("dt"))
	assert.False(t, hit.Request.QueryParams.Has("dr"))
	assert.Equal(t, long, hit.Request.QueryParams.Get("ep.long"))
}

func TestReplaceURLParameterTarget(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.QueryParams.Set("dl", "https://example.com/?ref=jan%40example.com&a=1")
	p := compileRules(t, rule{Expression: "true", Actions: []action{
		replace(urlParameter("ref"), emailPattern, nil),
	}})
	// when
	_, err := apply(p, "ga4", hit, nil, ExtractionDependencies{})
	// then
	require.NoError(t, err)
	parsed, err := url.Parse(hit.Request.QueryParams.Get("dl"))
	require.NoError(t, err)
	assert.Equal(t, "[REDACTED]", parsed.Query().Get("ref"))
}

func TestReplaceCompileErrors(t *testing.T) {
	for name, actions := range map[string][]action{
		"number target":    {replace(parameter("n", "number"), "x", nil)},
		"empty pattern":    {replace(field("page_title"), "", nil)},
		"invalid pattern":  {replace(field("page_title"), "(", nil)},
		"lookahead":        {replace(field("page_title"), "a(?=b)", nil)},
		"too long pattern": {replace(field("page_title"), strings.Repeat("a", MaxPatternLength+1), nil)},
		"value given": {{Type: "replace", Target: field("page_title"), Pattern: "a",
			Value: &literal{String: text("x")}}},
		"pattern on set":      {{Type: "set", Target: field("page_title"), Pattern: "a", Value: &literal{String: text("x")}}},
		"read-only target":    {replace(field("measurement_id"), "a", nil)},
		"duplicate target":    {replace(field("page_title"), "a", nil), set(field("page_title"), "x")},
		"url with page parts": {replace(field("page_location"), "a", nil), replace(urlParameter("x"), "a", nil)},
	} {
		t.Run(name, func(t *testing.T) {
			// when
			_, err := Compile(properties.TransformationsConfig{Rules: []rule{{Expression: "true", Actions: actions}}})
			// then
			assert.Error(t, err)
		})
	}
}

const benchmarkURL = "https://shop.example.com/checkout/thank-you?utm_source=newsletter&utm_medium=email" +
	"&utm_campaign=autumn_sale&edition=42&gclid=TeSt123"

func benchmarkReplace(b *testing.B, pattern, replacement, location string) {
	p, err := Compile(properties.TransformationsConfig{Rules: []rule{{Expression: "true", Actions: []action{
		replace(field("page_location"), pattern, text(replacement)),
	}}}})
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		hit := hits.New()
		hit.Request.QueryParams.Set("dl", location)
		if _, err := p.Apply(Input{Protocol: "ga4", Hit: hit}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplacePersonalDataNoMatch(b *testing.B) {
	benchmarkReplace(b, piiPattern, "[REDACTED]", benchmarkURL)
}

func BenchmarkReplacePersonalDataEncodedEmail(b *testing.B) {
	benchmarkReplace(b, piiPattern, "[REDACTED]", benchmarkURL+"&email=jan.kowalski%40example.com")
}

func BenchmarkReplaceEmailOnly(b *testing.B) {
	benchmarkReplace(b, emailPattern, "[REDACTED]", benchmarkURL+"&email=jan.kowalski%40example.com")
}

func BenchmarkReplaceIDs(b *testing.B) {
	benchmarkReplace(b, idsPattern, "$1:id$2", "https://shop.example.com/orders/1234567/items?x=1")
}

// Baseline: the same rule shape without regex work, to isolate the pattern cost.
func BenchmarkReplaceBaselineSetLiteral(b *testing.B) {
	p, err := Compile(properties.TransformationsConfig{Rules: []rule{{Expression: "true", Actions: []action{
		set(field("page_title"), "x"),
	}}}})
	require.NoError(b, err)
	b.ReportAllocs()
	for b.Loop() {
		hit := hits.New()
		hit.Request.QueryParams.Set("dl", benchmarkURL)
		if _, err := p.Apply(Input{Protocol: "ga4", Hit: hit}); err != nil {
			b.Fatal(err)
		}
	}
}
