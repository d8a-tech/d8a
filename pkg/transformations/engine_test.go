package transformations

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strings"
	"testing"

	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/splitter"
	"github.com/expr-lang/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ref = properties.TransformationFieldReference
type action = properties.TransformationActionConfig
type literal = properties.TransformationLiteral
type rule = properties.TransformationRuleConfig

func field(name string) *ref           { return &ref{Field: name} }
func parameter(name, kind string) *ref { return &ref{Parameter: name, Type: kind} }
func set(target *ref, value string) action {
	return action{Type: "set", Target: target, Value: &literal{String: &value}}
}
func number(target *ref, value float64) action {
	return action{Type: "set", Target: target, Value: &literal{Number: &value}}
}
func compileRules(t *testing.T, rules ...rule) Program {
	t.Helper()
	p, err := Compile(properties.TransformationsConfig{Rules: rules})
	require.NoError(t, err)
	return p
}

func TestSequentialRulesAndCopySnapshot(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.QueryParams.Set("dt", "title")
	hit.Request.QueryParams.Set("dr", "referrer")
	p := compileRules(t,
		rule{Expression: "true", Actions: []action{
			{Type: "copy", Target: field("page_title"), Source: field("page_referrer")},
			{Type: "copy", Target: field("page_referrer"), Source: field("page_title")},
		}},
		rule{Expression: `page_title == "referrer" && page_referrer == "title"`, Actions: []action{
			set(field("page_title"), "overwritten"), set(field("name"), "renamed"), set(field("user_id"), "new-user"),
		}},
		rule{Expression: `name == "renamed" && user_id == "new-user"`, Actions: []action{
			set(field("page_title"), "last"),
		}},
	)
	// when
	keep, err := p.Apply("ga4", hit, nil, ExtractionDependencies{})
	// then
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Equal(t, "last", hit.Request.QueryParams.Get("dt"))
	assert.Equal(t, "title", hit.Request.QueryParams.Get("dr"))
	assert.Equal(t, "renamed", hit.EventName)
	assert.Equal(t, "renamed", hit.Request.QueryParams.Get("en"))
	require.NotNil(t, hit.UserID)
	assert.Equal(t, "new-user", *hit.UserID)
}

func TestPredicateCompatibility(t *testing.T) {
	for _, expression := range []string{
		`(page_title == "hello" and name == "page_view") or page_title == "other"`,
		`page_title != nil && page_title contains "ell" && page_title startsWith "he" && page_title endsWith "lo"`,
		`page_title matches "^he.*o$" && name in ["page_view", "purchase"]`,
		`params.missing == nil && numeric_params.missing == nil && page_referrer == nil`,
		`numeric_params.score != nil && numeric_params.score >= 2 && numeric_params.score < 4`,
		`params.address != nil && inCidr(params.address, "192.168.0.0/16")`,
	} {
		t.Run(expression, func(t *testing.T) {
			// given
			hit := hits.New()
			hit.EventName = "page_view"
			hit.Request.QueryParams.Set("dt", "hello")
			hit.Request.QueryParams.Set("epn.score", "3")
			hit.Request.QueryParams.Set("ep.address", "192.168.1.3")
			p := compileRules(t, rule{Expression: expression, Actions: []action{{Type: "drop"}}})
			// when
			keep, err := p.Apply("ga4", hit, nil, ExtractionDependencies{})
			// then
			require.NoError(t, err)
			assert.False(t, keep)
		})
	}
}

func TestInCidrFilterParity(t *testing.T) {
	for _, address := range []string{"192.168.1.3", "10.0.0.1", "not-an-ip", "2001:db8::1"} {
		// given
		expression := fmt.Sprintf(`inCidr(%q, "192.168.0.0/16")`, address)
		filter, err := expr.Compile(expression, append(splitter.FunctionOptions(), expr.AsBool())...)
		require.NoError(t, err)
		p := compileRules(t, rule{Expression: expression, Actions: []action{{Type: "drop"}}})
		// when
		matched, err := expr.Run(filter, nil)
		require.NoError(t, err)
		keep, err := p.Apply("ga4", hits.New(), nil, ExtractionDependencies{})
		// then
		require.NoError(t, err)
		assert.Equal(t, matched, !keep)
	}
}

func TestMissingEmptyAndParameterNamespaces(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.QueryParams.Set("ep.empty", "")
	hit.Request.QueryParams.Set("ep.score", "old")
	p := compileRules(t,
		rule{Expression: `params.empty != nil && params.empty == "" && params.missing == nil`, Actions: []action{
			{Type: "copy", Target: field("page_title"), Source: parameter("empty", "string")},
			{Type: "copy", Target: field("page_referrer"), Source: parameter("missing", "string")},
			number(parameter("score", "number"), 0),
		}},
		rule{Expression: `numeric_params.score == 0 && params.score == nil`, Actions: []action{
			{Type: "remove", Target: parameter("score", "number")},
			{Type: "remove", Target: field("user_id")},
		}},
	)
	// when
	keep, err := p.Apply("d8a", hit, nil, ExtractionDependencies{})
	// then
	require.NoError(t, err)
	assert.True(t, keep)
	assert.True(t, hit.Request.QueryParams.Has("dt"))
	assert.Empty(t, hit.Request.QueryParams.Get("dt"))
	assert.False(t, hit.Request.QueryParams.Has("dr"))
	assert.False(t, hit.Request.QueryParams.Has("ep.score"))
	assert.False(t, hit.Request.QueryParams.Has("epn.score"))
	assert.Nil(t, hit.UserID)
}

func TestProtocolWrites(t *testing.T) {
	for _, tc := range []struct {
		id, titleKey, locationKey, referrerKey string
		custom                                 bool
	}{
		{"ga4", "dt", "dl", "dr", true}, {"d8a", "dt", "dl", "dr", true},
		{"matomo", "action_name", "url", "urlref", false}, {"unknown", "", "", "", false},
	} {
		t.Run(tc.id, func(t *testing.T) {
			// given
			hit := hits.New()
			hit.EventName = "original"
			p := compileRules(t, rule{Expression: "true", Actions: []action{
				set(field("name"), "changed"), set(field("page_title"), "title"),
				set(field("page_location"), "https://example.com"), set(field("page_referrer"), "referrer"),
				set(field("user_id"), "user"), set(parameter("test", "string"), "custom"),
			}})
			// when
			keep, err := p.Apply(tc.id, hit, nil, ExtractionDependencies{})
			// then
			require.NoError(t, err)
			assert.True(t, keep)
			assert.Equal(t, tc.custom, hit.Request.QueryParams.Has("en"))
			assert.Equal(t, tc.custom, hit.Request.QueryParams.Has("ep.test"))
			if tc.titleKey != "" {
				assert.Equal(t, "title", hit.Request.QueryParams.Get(tc.titleKey))
				assert.Equal(t, "https://example.com", hit.Request.QueryParams.Get(tc.locationKey))
				assert.Equal(t, "referrer", hit.Request.QueryParams.Get(tc.referrerKey))
				assert.Equal(t, "user", hit.Request.QueryParams.Get("uid"))
			}
		})
	}
}

func TestURLExclusionAndDerivedInvalidation(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.QueryParams.Set("dl", "https://example.com/old?secret=canary&keep=yes")
	p := compileRules(t,
		rule{Expression: `page_location == "https://example.com/old?keep=yes" && page_path == "/old"`, Actions: []action{
			set(field("page_location"), "https://new.example/new?secret=other&keep=no"),
		}},
		rule{Expression: `page_hostname == "new.example" && page_path == "/new" && ` +
			`page_location == "https://new.example/new?keep=no"`,
			Actions: []action{{Type: "drop"}}},
	)
	// when
	keep, err := p.Apply("ga4", hit, &properties.Settings{ExcludedURLParams: []string{"secret"}}, ExtractionDependencies{})
	// then
	require.NoError(t, err)
	assert.False(t, keep)
}

type geoStub struct {
	calls  int
	ip     netip.Addr
	err    error
	record *dbip.LookupResult
}

func (g *geoStub) Lookup(ip netip.Addr) (*dbip.LookupResult, error) {
	g.calls++
	g.ip = ip
	return g.record, g.err
}

func TestGeoAndPayloadFreeErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		lookup     *geoStub
		expression string
		wantErr    bool
		calls      int
	}{
		{"unavailable dependency", nil, `geo_country == nil`, false, 0},
		{"unavailable database", &geoStub{err: dbip.ErrUnavailable}, `geo_country == nil && geo_city == nil`, false, 1},
		{"lookup error", &geoStub{err: errors.New("secret-canary")}, `geo_country == "NL"`, true, 1},
		{"record", &geoStub{record: &dbip.LookupResult{Country: "NL", City: "Amsterdam"}},
			`geo_country == "NL" && geo_city == "Amsterdam" && geo_region == nil`, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			hit := hits.New()
			hit.Request.IP = "192.168.1.0"
			deps := ExtractionDependencies{}
			if tc.lookup != nil {
				deps.Geo = tc.lookup
			}
			p := compileRules(t, rule{Expression: tc.expression, Actions: []action{{Type: "drop"}}})
			// when
			keep, err := p.Apply("ga4", hit, nil, deps)
			// then
			assert.False(t, keep)
			assert.Equal(t, tc.wantErr, err != nil)
			if err != nil {
				assert.NotContains(t, err.Error(), "secret-canary")
			}
			if tc.lookup != nil {
				assert.Equal(t, tc.calls, tc.lookup.calls)
				assert.Equal(t, "192.168.1.0", tc.lookup.ip.String())
			}
		})
	}
}

func TestRuleMutationIsAtomicOnSourceError(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.IP = "192.168.1.0"
	hit.Request.QueryParams.Set("dt", "original")
	p := compileRules(t, rule{Expression: "true", Actions: []action{
		set(field("page_title"), "changed"), {Type: "copy", Target: field("page_referrer"), Source: field("geo_city")},
	}})
	// when
	_, err := p.Apply("ga4", hit, nil, ExtractionDependencies{Geo: &geoStub{err: errors.New("canary")}})
	// then
	require.Error(t, err)
	assert.Equal(t, "original", hit.Request.QueryParams.Get("dt"))
}

func TestCompileRejectsInvalidConfiguration(t *testing.T) {
	for _, expression := range []string{
		`name`, `42`, `missing == nil`, `name == 2`, `name + 1 == 3`, `name matches "["`,
		`numeric_params.score contains "x"`, `name invalid "x"`, `hit.Request != nil`,
		strings.Repeat(" ", MaxExpressionLength+1), "",
	} {
		// given
		config := properties.TransformationsConfig{Rules: []rule{{Expression: expression, Actions: []action{{Type: "drop"}}}}}
		// when
		_, err := Compile(config)
		// then
		assert.Error(t, err, expression)
	}
	for _, actions := range [][]action{
		{set(field("page_title"), "a"), set(field("dt"), "b")},
		{set(parameter("x", "string"), "a"), number(parameter("x", "number"), 2)},
		{{Type: "drop"}, set(field("page_title"), "x")},
		{{Type: "remove", Target: field("name")}},
		{set(field("page_hostname"), "x")}, {set(field("ip_address"), "x")},
		{set(parameter("x", "number"), "text")}, {number(parameter("x", "number"), math.Inf(1))},
		{{Type: "copy", Source: parameter("x", "number"), Target: field("page_title")}},
		{{Type: "bogus", Target: field("page_title")}},
		{set(field("page_title"), strings.Repeat("x", MaxValueLength+1))},
		{{Type: "set", Target: field("page_title"), Value: &literal{}}},
		{{Type: "copy", Target: field("page_title")}},
		{{Type: "remove", Target: &ref{Field: "page_title", Parameter: "x", Type: "string"}}},
		make([]action, MaxActions+1), nil,
	} {
		// given
		config := properties.TransformationsConfig{Rules: []rule{{Expression: "true", Actions: actions}}}
		// when
		_, err := Compile(config)
		// then
		assert.Error(t, err, "%+v", actions)
	}
}

func TestLimitsAndCacheSnapshot(t *testing.T) {
	// given
	config := properties.TransformationsConfig{Rules: []rule{{
		Name: strings.Repeat("n", MaxNameLength), Expression: "true",
		Actions: []action{set(field("page_title"), strings.Repeat("v", MaxValueLength))},
	}}}
	p, err := Compile(config)
	require.NoError(t, err)
	config.Rules[0].Name = "another name"
	// when
	cached, err := Compile(config)
	// then
	require.NoError(t, err)
	assert.Same(t, p, cached)
	*config.Rules[0].Actions[0].Value.String = "mutated"
	hit := hits.New()
	_, err = p.Apply("ga4", hit, nil, ExtractionDependencies{})
	require.NoError(t, err)
	assert.Len(t, hit.Request.QueryParams.Get("dt"), MaxValueLength)
	config.Rules[0].Name = strings.Repeat("n", MaxNameLength+1)
	_, err = Compile(config)
	require.Error(t, err)
	_, err = Compile(properties.TransformationsConfig{Rules: make([]rule, MaxRules+1)})
	require.Error(t, err)
	for i := 0; i < MaxCachedPrograms+1; i++ {
		compileRules(t, rule{Expression: "true", Actions: []action{set(field("page_title"), fmt.Sprint(i))}})
	}
	compiledCache.Lock()
	defer compiledCache.Unlock()
	assert.LessOrEqual(t, len(compiledCache.programs), MaxCachedPrograms)
	assert.Len(t, compiledCache.order, len(compiledCache.programs))
}

func TestEvaluationErrorIsPayloadFree(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.QueryParams.Set("dt", "secret-canary")
	hit.Request.QueryParams.Set("ep.pattern", "[")
	p := compileRules(t, rule{Expression: `page_title matches params.pattern`, Actions: []action{{Type: "drop"}}})
	// when
	keep, err := p.Apply("ga4", hit, nil, ExtractionDependencies{})
	// then
	assert.False(t, keep)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret-canary")
}
