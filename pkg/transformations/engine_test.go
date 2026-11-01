package transformations

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strings"
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/d8a"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/protocol/matomo"
	"github.com/d8a-tech/d8a/pkg/schema"
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
func urlParameter(name string) *ref    { return &ref{URLParameter: name} }
func set(target *ref, value string) action {
	return action{Type: "set", Target: target, Value: &literal{String: &value}}
}
func number(target *ref, value float64) action {
	return action{Type: "set", Target: target, Value: &literal{Number: &value}}
}
func template(target *ref, value string) action {
	return action{Type: "set", Target: target, Value: &literal{Template: &value}}
}
func remove(target *ref) action { return action{Type: "remove", Target: target} }
func compileRules(t *testing.T, rules ...rule) Program {
	t.Helper()
	p, err := Compile(properties.TransformationsConfig{Rules: rules})
	require.NoError(t, err)
	return p
}

func testProtocols(registry properties.SettingsRegistry) []protocol.Protocol {
	converter := currency.NewDummyConverter(1)
	return []protocol.Protocol{
		ga4.NewGA4Protocol(converter, registry), d8a.NewD8AProtocol(converter, registry),
		matomo.NewMatomoProtocol(matomo.NewFromIDSiteExtractor(registry), registry),
	}
}

// columnDeps builds the same column registries the worker uses.
func columnDeps(settings *properties.Settings, geo dbip.LookupProvider) ExtractionDependencies {
	if settings == nil {
		settings = &properties.Settings{}
	}
	registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(settings))
	var options []columnset.ColumnSetOption
	if geo != nil {
		options = append(options, columnset.WithGeoProvider(geo))
	}
	result := make(map[string]schema.ColumnsRegistry)
	for _, implementation := range testProtocols(registry) {
		result[implementation.ID()] = columnset.DefaultColumnRegistry(implementation, registry, options...)
	}
	return ExtractionDependencies{Columns: result}
}

func apply(p Program, protocolID string, hit *hits.Hit, settings *properties.Settings, deps ExtractionDependencies,
) (bool, error) {
	return p.Apply(Input{Protocol: protocolID, Hit: hit, Settings: settings, Dependencies: deps})
}

func TestSequentialRulesAndTemplateSnapshot(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.QueryParams.Set("dt", "title")
	hit.Request.QueryParams.Set("dr", "referrer")
	p := compileRules(t,
		rule{Expression: "true", Actions: []action{
			template(field("page_title"), "{{page_referrer}}"),
			template(field("page_referrer"), "{{page_title}}"),
		}},
		rule{Expression: `page_title == "referrer" && page_referrer == "title"`, Actions: []action{
			set(field("page_title"), "overwritten"), set(field("name"), "renamed"), set(field("user_id"), "new-user"),
		}},
		rule{Expression: `name == "renamed" && user_id == "new-user"`, Actions: []action{
			template(field("page_title"), "{{page_title}} | {{name}}"),
		}},
	)
	// when
	keep, err := apply(p, "ga4", hit, nil, columnDeps(nil, nil))
	// then
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Equal(t, "overwritten | renamed", hit.Request.QueryParams.Get("dt"))
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
		`url_params.utm_source == "news" && utm_source == "news" && url_params.missing == nil`,
		`ip_address != nil && inCidr(ip_address, "10.1.2.0/24")`,
	} {
		t.Run(expression, func(t *testing.T) {
			// given
			hit := hits.New()
			hit.EventName = "page_view"
			hit.Request.IP = "10.1.2.0"
			hit.Request.QueryParams.Set("dt", "hello")
			hit.Request.QueryParams.Set("dl", "https://example.com/?utm_source=news")
			hit.Request.QueryParams.Set("epn.score", "3")
			hit.Request.QueryParams.Set("ep.address", "192.168.1.3")
			p := compileRules(t, rule{Expression: expression, Actions: []action{{Type: "drop"}}})
			// when
			keep, err := p.Apply(Input{
				Protocol: "ga4", Hit: hit, OriginalIP: "10.1.2.3", Dependencies: columnDeps(nil, nil),
			})
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
		keep, err := apply(p, "ga4", hits.New(), nil, ExtractionDependencies{})
		// then
		require.NoError(t, err)
		assert.Equal(t, matched, !keep)
	}
}

func TestIPAddressIsUnmaskedAndConditionOnly(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.IP = "10.1.2.0" // masked by the receiver before transformations run
	p := compileRules(t, rule{Expression: `ip_address == "10.1.2.3"`, Actions: []action{{Type: "drop"}}})
	// when
	keep, err := p.Apply(Input{Protocol: "ga4", Hit: hit, OriginalIP: "10.1.2.3"})
	// then
	require.NoError(t, err)
	assert.False(t, keep)
	assert.Equal(t, "10.1.2.0", hit.Request.IP)
	for _, actions := range [][]action{
		{set(field("ip_address"), "x")}, {template(field("page_title"), "{{ip_address}}")},
	} {
		_, err := Compile(properties.TransformationsConfig{Rules: []rule{{Expression: "true", Actions: actions}}})
		assert.Error(t, err)
	}
}

func TestMissingEmptyAndParameterNamespaces(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.QueryParams.Set("ep.empty", "")
	hit.Request.QueryParams.Set("ep.score", "old")
	p := compileRules(t,
		rule{Expression: `params.empty != nil && params.empty == "" && params.missing == nil`, Actions: []action{
			template(field("page_title"), "{{params.empty}}"),
			template(field("page_referrer"), "{{params.missing}}"),
			number(parameter("score", "number"), 0),
		}},
		rule{Expression: `numeric_params.score == 0 && params.score == nil`, Actions: []action{
			remove(parameter("score", "number")),
			remove(field("user_id")),
		}},
	)
	// when
	keep, err := apply(p, "d8a", hit, nil, columnDeps(nil, nil))
	// then
	require.NoError(t, err)
	assert.True(t, keep)
	assert.True(t, hit.Request.QueryParams.Has("dt"))
	assert.Empty(t, hit.Request.QueryParams.Get("dt"))
	assert.False(t, hit.Request.QueryParams.Has("dr"), "a single missing reference skips the action")
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
			keep, err := apply(p, tc.id, hit, nil, columnDeps(nil, nil))
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
	settings := &properties.Settings{ExcludedURLParams: []string{"secret"}}
	for name, deps := range map[string]ExtractionDependencies{
		"columns": columnDeps(settings, nil), "core fallback": {},
	} {
		t.Run(name, func(t *testing.T) {
			// given
			hit := hits.New()
			hit.Request.QueryParams.Set("dl", "https://example.com/old?secret=canary&keep=yes")
			p := compileRules(t,
				rule{Expression: `page_location == "https://example.com/old?keep=yes" && page_path == "/old"`,
					Actions: []action{set(field("page_location"), "https://new.example/new?secret=other&keep=no")}},
				rule{Expression: `page_hostname == "new.example" && page_path == "/new" && ` +
					`page_location == "https://new.example/new?keep=no"`,
					Actions: []action{{Type: "drop"}}},
			)
			// when
			keep, err := apply(p, "ga4", hit, settings, deps)
			// then
			require.NoError(t, err)
			assert.False(t, keep)
		})
	}
}

func TestRuleWritesInvalidateReadsForLaterRules(t *testing.T) {
	for name, deps := range map[string]ExtractionDependencies{
		"columns": columnDeps(nil, nil), "core fallback": {},
	} {
		t.Run(name, func(t *testing.T) {
			// given
			hit := hits.New()
			hit.Request.QueryParams.Set("dl", "https://example.com/old?keep=yes")
			hit.Request.QueryParams.Set("dt", "title")
			p := compileRules(t,
				// Reads before any write, so the view memoizes the original values.
				rule{Expression: `page_path == "/old" && page_hostname == "example.com" && page_title == "title"`,
					Actions: []action{
						set(field("page_path"), "/new"),
						template(parameter("previous_path", "string"), "{{page_path}}"),
						set(field("page_hostname"), "new.example"),
					}},
				// Param-only write: the next rule must still see it.
				rule{Expression: `page_path == "/new" && page_hostname == "new.example" && ` +
					`params.previous_path == "/old"`,
					Actions: []action{set(parameter("checked", "string"), "yes")}},
				rule{Expression: `params.checked == "yes"`, Actions: []action{set(field("page_title"), "done")}},
			)
			// when
			keep, err := apply(p, "ga4", hit, nil, deps)
			// then
			require.NoError(t, err)
			assert.True(t, keep)
			assert.Equal(t, "https://new.example/new?keep=yes", hit.Request.QueryParams.Get("dl"))
			assert.Equal(t, "/old", hit.Request.QueryParams.Get("ep.previous_path"))
			assert.Equal(t, "yes", hit.Request.QueryParams.Get("ep.checked"))
			assert.Equal(t, "done", hit.Request.QueryParams.Get("dt"))
		})
	}
}

func TestPageURLLeftAloneWhenNothingChanges(t *testing.T) {
	// A raw URL whose query string would be re-encoded and reordered if re-serialized.
	const raw = "https://example.com/a?z=1&mail=a@example.com&phone=%2B48%20601&tag=[x]"
	cases := []struct {
		name    string
		actions []action
		want    string
	}{
		{name: "remove missing click ID", actions: []action{remove(field("click_id_gclid"))}, want: raw},
		{name: "remove missing URL parameter", actions: []action{remove(urlParameter("token"))}, want: raw},
		{name: "set URL parameter to its value", actions: []action{set(urlParameter("z"), "1")}, want: raw},
		{name: "set path to its value", actions: []action{set(field("page_path"), "/a")}, want: raw},
		{name: "set hostname to its value", actions: []action{set(field("page_hostname"), "example.com")}, want: raw},
		{
			name:    "remove present URL parameter rewrites",
			actions: []action{remove(urlParameter("z"))},
			want:    "https://example.com/a?mail=a%40example.com&phone=%2B48+601&tag=%5Bx%5D",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			hit := hits.New()
			hit.Request.QueryParams.Set("dl", raw)
			p := compileRules(t, rule{Expression: "true", Actions: tc.actions})
			// when
			keep, err := apply(p, "ga4", hit, nil, columnDeps(nil, nil))
			// then
			require.NoError(t, err)
			assert.True(t, keep)
			assert.Equal(t, tc.want, hit.Request.QueryParams.Get("dl"))
		})
	}
}

func TestPageURLWrites(t *testing.T) {
	for _, protocolID := range []string{"ga4", "matomo"} {
		t.Run(protocolID, func(t *testing.T) {
			// given
			key := map[string]string{"ga4": "dl", "matomo": "url"}[protocolID]
			hit := hits.New()
			hit.Request.QueryParams.Set(key, "https://example.com/a?utm_source=old&gclid=1&keep=yes")
			p := compileRules(t,
				rule{Expression: `utm_source == "old"`, Actions: []action{
					template(field("utm_source"), "{{utm_source}}-new"), remove(field("click_id_gclid")),
					set(urlParameter("ref"), "x"), set(field("page_path"), "/b"),
				}},
				rule{Expression: `utm_source == "old-new" && click_id_gclid == nil && url_params.ref == "x" && ` +
					`page_path == "/b"`, Actions: []action{set(field("page_hostname"), "other.example")}},
			)
			// when
			keep, err := apply(p, protocolID, hit, nil, columnDeps(nil, nil))
			// then
			require.NoError(t, err)
			assert.True(t, keep)
			assert.Equal(t, "https://other.example/b?keep=yes&ref=x&utm_source=old-new",
				hit.Request.QueryParams.Get(key))
		})
	}
	// A rule cannot rewrite the whole URL and parts of it at once.
	_, err := Compile(properties.TransformationsConfig{Rules: []rule{{Expression: "true", Actions: []action{
		set(field("page_location"), "https://example.com"), set(field("utm_source"), "x"),
	}}}})
	assert.Error(t, err)
	// A missing page URL is left alone.
	hit := hits.New()
	p := compileRules(t, rule{Expression: "true", Actions: []action{set(field("utm_source"), "x")}})
	keep, err := apply(p, "ga4", hit, nil, columnDeps(nil, nil))
	require.NoError(t, err)
	assert.True(t, keep)
	assert.False(t, hit.Request.QueryParams.Has("dl"))
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
		{"unavailable database", &geoStub{err: dbip.ErrUnavailable}, `geo_country == nil && geo_city == nil`, false, 2},
		{"lookup error", &geoStub{err: errors.New("secret-canary")}, `geo_country == "NL"`, true, 1},
		{"record", &geoStub{record: &dbip.LookupResult{Country: "NL", City: "Amsterdam"}},
			`geo_country == "NL" && geo_city == "Amsterdam" && geo_region == nil`, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			hit := hits.New()
			hit.Request.IP = "192.168.1.0"
			var provider dbip.LookupProvider
			if tc.lookup != nil {
				provider = tc.lookup
			}
			p := compileRules(t, rule{Expression: tc.expression, Actions: []action{{Type: "drop"}}})
			// when
			keep, err := apply(p, "ga4", hit, nil, columnDeps(nil, provider))
			// then
			assert.False(t, keep)
			assert.Equal(t, tc.wantErr, err != nil)
			if err != nil {
				assert.NotContains(t, err.Error(), "secret-canary")
				var failure *Error
				require.ErrorAs(t, err, &failure)
				assert.Equal(t, StageExtraction, failure.Stage)
				assert.Equal(t, 1, failure.Rule)
			}
			if tc.lookup != nil {
				assert.Equal(t, tc.calls, tc.lookup.calls)
				assert.Equal(t, "192.168.1.0", tc.lookup.ip.String())
			}
		})
	}
}

func TestLazyExtractionShortCircuits(t *testing.T) {
	// given
	lookup := &geoStub{record: &dbip.LookupResult{Country: "NL"}}
	deps := columnDeps(nil, lookup)
	p := compileRules(t, rule{Expression: `name == "purchase" && geo_country == "NL"`,
		Actions: []action{{Type: "drop"}}})
	hit := hits.New()
	hit.EventName = "page_view"
	hit.Request.IP = "192.168.1.0"
	// when
	keep, err := apply(p, "ga4", hit, nil, deps)
	// then
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Equal(t, 0, lookup.calls, "geo must not be looked up when an earlier condition decides")
	hit.EventName = "purchase"
	keep, err = apply(p, "ga4", hit, nil, deps)
	require.NoError(t, err)
	assert.False(t, keep)
	assert.Equal(t, 1, lookup.calls)
}

func TestGeoOverrides(t *testing.T) {
	// given
	lookup := &geoStub{record: &dbip.LookupResult{Country: "NL", City: "Amsterdam", Region: "NH"}}
	deps := columnDeps(nil, lookup)
	hit := hits.New()
	hit.Request.IP = "192.168.1.0"
	p := compileRules(t,
		rule{Expression: `geo_country == "NL"`, Actions: []action{
			set(field("geo_country"), "PL"), remove(field("geo_city")),
		}},
		rule{Expression: `geo_country == "PL" && geo_city == nil && geo_region == "NH"`, Actions: []action{
			template(parameter("country", "string"), "{{geo_country}}"),
		}},
	)
	// when
	keep, err := apply(p, "ga4", hit, nil, deps)
	// then
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Equal(t, "PL", hit.Request.QueryParams.Get("ep.country"))
	assert.Equal(t, "PL", hit.Metadata[columns.OverrideMetadataKey("geo_country")])
	assert.Equal(t, "1", hit.Metadata[columns.UnsetMetadataKey("geo_city")])
	// The worker's geo columns honour the overrides carried by the hit.
	cols, err := deps.Columns["ga4"].Get("")
	require.NoError(t, err)
	event := schema.NewEvent(hit)
	for _, column := range cols.Event {
		switch column.Implements().Field.Name {
		case "geo_country", "geo_city", "geo_region":
			require.NoError(t, column.Write(event))
		}
	}
	assert.Equal(t, "PL", event.Values["geo_country"])
	assert.Nil(t, event.Values["geo_city"])
	assert.Equal(t, "NH", event.Values["geo_region"])
}

func TestTemplates(t *testing.T) {
	for _, tc := range []struct {
		name, template, want string
	}{
		{"concatenation", "{{page_title}} - {{params.section}} ({{numeric_params.count}})", "Home - news (3)"},
		{"missing parts are empty", "[{{params.missing}}]", "[]"},
		{"escaped braces", `\{{page_title}}`, "{{page_title}}"},
		{"url parameter", "{{url_params.q}}", "shoes"},
		{"empty template", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			hit := hits.New()
			hit.Request.QueryParams.Set("dt", "Home")
			hit.Request.QueryParams.Set("dl", "https://example.com/?q=shoes")
			hit.Request.QueryParams.Set("ep.section", "news")
			hit.Request.QueryParams.Set("epn.count", "3")
			p := compileRules(t, rule{Expression: "true", Actions: []action{template(parameter("out", "string"), tc.template)}})
			// when
			_, err := apply(p, "ga4", hit, nil, columnDeps(nil, nil))
			// then
			require.NoError(t, err)
			assert.Equal(t, tc.want, hit.Request.QueryParams.Get("ep.out"))
		})
	}
	// Number targets accept exactly one number reference.
	hit := hits.New()
	hit.Request.QueryParams.Set("epn.count", "3")
	p := compileRules(t, rule{Expression: "true", Actions: []action{
		template(parameter("copy", "number"), "{{numeric_params.count}}"),
	}})
	_, err := apply(p, "ga4", hit, nil, ExtractionDependencies{})
	require.NoError(t, err)
	assert.Equal(t, "3", hit.Request.QueryParams.Get("epn.copy"))
	for _, invalid := range []string{"{{page_title}}", "x{{numeric_params.count}}", "{{params.count}}"} {
		_, err := Compile(properties.TransformationsConfig{Rules: []rule{{Expression: "true", Actions: []action{
			template(parameter("copy", "number"), invalid),
		}}}})
		assert.Error(t, err, invalid)
	}
	for _, invalid := range []string{"{{page_title", "{{unknown_field}}", "{{device_category}}"} {
		_, err := Compile(properties.TransformationsConfig{Rules: []rule{{Expression: "true", Actions: []action{
			template(field("page_title"), invalid),
		}}}})
		assert.Error(t, err, invalid)
	}
}

func TestRuleMutationIsAtomicOnSourceError(t *testing.T) {
	// given
	hit := hits.New()
	hit.Request.IP = "192.168.1.0"
	hit.Request.QueryParams.Set("dt", "original")
	p := compileRules(t, rule{Expression: "true", Actions: []action{
		set(field("page_title"), "changed"), template(field("page_referrer"), "{{geo_city}}"),
	}})
	// when
	_, err := apply(p, "ga4", hit, nil, columnDeps(nil, &geoStub{err: errors.New("canary")}))
	// then
	require.Error(t, err)
	assert.Equal(t, "original", hit.Request.QueryParams.Get("dt"))
}

func TestCompileRejectsInvalidConfiguration(t *testing.T) {
	for _, expression := range []string{
		`name`, `42`, `missing == nil`, `name == 2`, `name + 1 == 3`, `name matches "["`,
		`numeric_params.score contains "x"`, `name invalid "x"`, `hit.Request != nil`,
		`device_category == "mobile"`, `device_web_browser == "Chrome"`,
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
		{set(urlParameter("x"), "a"), set(urlParameter("x"), "b")},
		{{Type: "drop"}, set(field("page_title"), "x")},
		{remove(field("name"))}, {remove(field("page_hostname"))},
		{set(field("measurement_id"), "x")}, {set(field("ip_address"), "x")},
		{set(field("device_category"), "x")},
		{set(parameter("x", "number"), "text")}, {number(parameter("x", "number"), math.Inf(1))},
		{{Type: "copy", Target: field("page_title")}},
		{{Type: "bogus", Target: field("page_title")}},
		{set(field("page_title"), strings.Repeat("x", MaxValueLength+1))},
		{{Type: "set", Target: field("page_title"), Value: &literal{}}},
		{{Type: "set", Target: field("page_title"), Value: &literal{String: new(string), Template: new(string)}}},
		{remove(&ref{Field: "page_title", Parameter: "x", Type: "string"})},
		{set(&ref{URLParameter: "x", Type: "number"}, "a")},
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
	_, err = apply(p, "ga4", hit, nil, ExtractionDependencies{})
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
	p := compileRules(t,
		rule{Expression: "true", Actions: []action{set(parameter("seen", "string"), "1")}},
		rule{Expression: `page_title matches params.pattern`, Actions: []action{{Type: "drop"}}},
	)
	// when
	keep, err := apply(p, "ga4", hit, nil, ExtractionDependencies{})
	// then
	assert.False(t, keep)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret-canary")
	var failure *Error
	require.ErrorAs(t, err, &failure)
	assert.Equal(t, StagePredicate, failure.Stage)
	assert.Equal(t, 2, failure.Rule)
}
