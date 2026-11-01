package receiver

import (
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"testing"

	"github.com/d8a-tech/d8a/pkg/columnset"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/transformations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func transformationSet(field, value string) properties.TransformationActionConfig {
	return properties.TransformationActionConfig{
		Type: "set", Target: &properties.TransformationFieldReference{Field: field},
		Value: &properties.TransformationLiteral{String: &value},
	}
}

func transformationTemplate(field, value string) properties.TransformationActionConfig {
	return properties.TransformationActionConfig{
		Type: "set", Target: &properties.TransformationFieldReference{Field: field},
		Value: &properties.TransformationLiteral{Template: &value},
	}
}

func transformationConfig(
	expression string, actions ...properties.TransformationActionConfig,
) *properties.TransformationsConfig {
	return &properties.TransformationsConfig{Rules: []properties.TransformationRuleConfig{{
		Expression: expression, Actions: actions,
	}}}
}

type batchSettingsRegistry struct {
	get func(string) (*properties.Settings, error)
}

func (s batchSettingsRegistry) GetByPropertyID(id string) (*properties.Settings, error) {
	return s.get(id)
}

func (s batchSettingsRegistry) GetByMeasurementID(id string) (*properties.Settings, error) {
	return s.get(id)
}

// geoDeps wires GA4 worker columns backed by the given geo provider.
func geoDeps(provider dbip.LookupProvider) transformations.ExtractionDependencies {
	registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{}))
	ga4Protocol := ga4.NewGA4Protocol(currency.NewDummyConverter(1), registry)
	return transformations.ExtractionDependencies{Columns: map[string]schema.ColumnsRegistry{
		"ga4": columnset.DefaultColumnRegistry(ga4Protocol, registry, columnset.WithGeoProvider(provider)),
	}}
}

type batchGeoLookup struct {
	lookup func(netip.Addr) (*dbip.LookupResult, error)
}

func (g batchGeoLookup) Lookup(ip netip.Addr) (*dbip.LookupResult, error) {
	return g.lookup(ip)
}

func TestTransformationBatchSnapshotAndRefresh(t *testing.T) {
	// given
	current := &properties.Settings{
		Transformations: transformationConfig(`geo_country == "NL"`, transformationSet("page_title", "captured")),
	}
	calls := map[string]int{}
	registry := batchSettingsRegistry{get: func(id string) (*properties.Settings, error) {
		calls[id]++
		return current, nil
	}}
	geo := batchGeoLookup{lookup: func(netip.Addr) (*dbip.LookupResult, error) {
		// Simulate a refresh after capture, during evaluation of the first hit.
		current.Transformations = transformationConfig("true", properties.TransformationActionConfig{Type: "drop"})
		return &dbip.LookupResult{Country: "NL"}, nil
	}}
	batch := []*hits.Hit{hits.New(), hits.New(), hits.New()}
	for i, hit := range batch {
		hit.PropertyID = []string{"one", "two", "one"}[i]
		hit.Request.IP = "192.0.2.0"
	}
	rule := NewTransformationBatchProcessingRule(registry, testCatalog, geoDeps(geo))
	// when
	retained, _, err := rule(&mockProtocol{id: "ga4"}, nil, batch)
	// then
	require.NoError(t, err)
	require.Len(t, retained, 3)
	assert.Equal(t, map[string]int{"one": 1, "two": 1}, calls)
	for _, hit := range retained {
		assert.Equal(t, "captured", hit.Request.QueryParams.Get("dt"))
	}
	// when: the next batch sees the refreshed configuration
	retained, _, err = rule(&mockProtocol{id: "ga4"}, nil, batch)
	// then
	require.NoError(t, err)
	assert.Empty(t, retained)
	assert.Equal(t, map[string]int{"one": 2, "two": 2}, calls)
}

func TestTransformationBatchOwnsMutableData(t *testing.T) {
	for _, protocolID := range []string{"ga4", "d8a"} {
		t.Run(protocolID, func(t *testing.T) {
			// given
			uid := "old-user"
			request := &hits.ParsedRequest{
				Body: []byte("original-batch"), Headers: http.Header{"User-Agent": {"original-agent"}},
				QueryParams: url.Values{"en": {"old-name"}, "uid": {uid}, "dt": {"old-title"}},
			}
			first := hits.NewWithServerAttributes(request)
			first.EventName, first.UserID = "old-name", &uid
			second := hits.NewWithServerAttributes(request)
			config := transformationConfig("true", transformationSet("name", "new-name"),
				transformationSet("user_id", "new-user"), transformationSet("page_title", "new-title"))
			registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{
				Transformations: config,
			}))
			// when
			retained, _, err := NewTransformationBatchProcessingRule(
				registry, testCatalog, transformations.ExtractionDependencies{},
			)(
				&mockProtocol{id: protocolID}, request, []*hits.Hit{first, second},
			)
			// then
			require.NoError(t, err)
			require.Len(t, retained, 2)
			for _, hit := range retained {
				assert.Equal(t, "new-name", hit.EventName)
				assert.Equal(t, hit.EventName, hit.Request.QueryParams.Get("en"))
				require.NotNil(t, hit.UserID)
				assert.Equal(t, *hit.UserID, hit.Request.QueryParams.Get("uid"))
				assert.Empty(t, hit.Request.Body)
			}
			retained[0].Request.Headers["User-Agent"][0] = "changed-agent"
			retained[0].Request.QueryParams["dt"][0] = "changed-title"
			retained[0].Metadata["extra"] = "owned"
			assert.Equal(t, "original-agent", request.Headers.Get("User-Agent"))
			assert.Equal(t, "original-agent", retained[1].Request.Headers.Get("User-Agent"))
			assert.Equal(t, "old-title", request.QueryParams.Get("dt"))
			assert.Equal(t, "new-title", retained[1].Request.QueryParams.Get("dt"))
			assert.Equal(t, "old-name", first.EventName)
			assert.Equal(t, "old-user", *first.UserID)
			assert.NotContains(t, first.Metadata, "extra")
			assert.Equal(t, "original-batch", string(request.Body))
		})
	}
}

func TestBatchProcessingOrder(t *testing.T) {
	// given
	var order []string
	p := &mockProtocol{id: "ga4", hits: []*hits.Hit{hits.New(), hits.New()}}
	raw := &capturingRawLogStorage{}
	s := NewServer(&mockStorage{}, raw, NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
		order = append(order, "validate:"+hit.EventName)
		return nil
	}), nil, 0, WithHitProcessingRule(NewSimpleHitProcessingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
		order = append(order, "process")
		hit.Request.IP = "192.0.2.0"
		return nil
	})), WithHitBatchProcessingRule(func(_ protocol.Protocol, original *hits.ParsedRequest, batch []*hits.Hit,
	) ([]*hits.Hit, bool, error) {
		order = append(order, "batch")
		assert.NotEqual(t, "192.0.2.0", original.IP, "the original request keeps the unmasked IP")
		for _, hit := range batch {
			assert.Equal(t, "192.0.2.0", hit.Request.IP)
			hit.EventName = "transformed"
		}
		return batch[1:], false, nil
	}))
	// when
	retained, err := s.createHits(&fasthttp.RequestCtx{}, p)
	// then
	require.NoError(t, err)
	assert.Len(t, retained, 1)
	assert.Equal(t, []string{"process", "process", "batch", "validate:transformed"}, order)
	assert.Len(t, raw.requests, 1)
}

func TestTransformationBatchRequestFailuresArePayloadFree(t *testing.T) {
	for _, tc := range []struct {
		name     string
		registry properties.SettingsRegistry
	}{
		{name: "missing registry"},
		{name: "settings lookup", registry: batchSettingsRegistry{get: func(string) (*properties.Settings, error) {
			return nil, errors.New("secret-canary")
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			hit := hits.New()
			// when
			retained, _, err := NewTransformationBatchProcessingRule(
				tc.registry, testCatalog, transformations.ExtractionDependencies{},
			)(
				&mockProtocol{id: "ga4"}, nil, []*hits.Hit{hit},
			)
			// then
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "secret")
			assert.Nil(t, retained)
		})
	}
}

func TestTransformationFailuresDropOnlyTheAffectedHit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *properties.TransformationsConfig
		deps   transformations.ExtractionDependencies
	}{
		{name: "compile", config: transformationConfig(`unknown_secret_canary`,
			properties.TransformationActionConfig{Type: "drop"})},
		{name: "evaluation", config: transformationConfig(`name == "bad" && split(name, "")[99] == "secret-canary"`,
			properties.TransformationActionConfig{Type: "drop"})},
		{name: "extraction", config: transformationConfig(`name == "bad" && geo_country == "secret-canary"`,
			properties.TransformationActionConfig{Type: "drop"}),
			deps: geoDeps(dbip.NewStaticLookupProvider(nil, errors.New("secret-canary")))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{
				Transformations: tc.config,
			}))
			bad, good := hits.New(), hits.New()
			bad.EventName, good.EventName = "bad", "good"
			bad.Request.IP, good.Request.IP = "192.0.2.0", "192.0.2.0"
			// when
			retained, _, err := NewTransformationBatchProcessingRule(registry, testCatalog, tc.deps)(
				&mockProtocol{id: "ga4"}, nil, []*hits.Hit{bad, good},
			)
			// then
			require.NoError(t, err)
			if tc.name == "compile" {
				assert.Empty(t, retained, "an invalid configuration drops every hit of its property")
				return
			}
			require.Len(t, retained, 1)
			assert.Equal(t, "good", retained[0].EventName)
		})
	}
}

func TestTransformationsSeeTheUnmaskedIP(t *testing.T) {
	// given
	registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{
		Transformations: transformationConfig(`inCidr(ip_address, "10.1.2.3/32")`,
			properties.TransformationActionConfig{Type: "drop"}),
	}))
	hit := hits.New()
	hit.Request.IP = "10.1.2.0"
	// when
	retained, _, err := NewTransformationBatchProcessingRule(
		registry, testCatalog, transformations.ExtractionDependencies{},
	)(
		&mockProtocol{id: "ga4"}, &hits.ParsedRequest{IP: "10.1.2.3"}, []*hits.Hit{hit},
	)
	// then
	require.NoError(t, err)
	assert.Empty(t, retained)
	assert.Equal(t, "10.1.2.0", hit.Request.IP)
}

func TestTransformationProgramsAreMemoizedPerConfiguration(t *testing.T) {
	// given
	memo := &programMemo{
		catalog: testCatalog, programs: make(map[*properties.TransformationsConfig]transformations.Program),
	}
	config := transformationConfig("true", transformationSet("page_title", "x"))
	// when
	first, err := memo.get(config)
	require.NoError(t, err)
	second, err := memo.get(config)
	require.NoError(t, err)
	// then
	assert.Same(t, first, second)
	assert.Len(t, memo.programs, 1)
}
