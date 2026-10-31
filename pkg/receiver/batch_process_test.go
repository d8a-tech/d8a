package receiver

import (
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"testing"

	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
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
	rule := NewTransformationBatchProcessingRule(registry, transformations.ExtractionDependencies{Geo: geo})
	// when
	retained, err := rule(&mockProtocol{id: "ga4"}, batch)
	// then
	require.NoError(t, err)
	require.Len(t, retained, 3)
	assert.Equal(t, map[string]int{"one": 1, "two": 1}, calls)
	for _, hit := range retained {
		assert.Equal(t, "captured", hit.Request.QueryParams.Get("dt"))
	}
	// when: the next batch sees the refreshed configuration
	retained, err = rule(&mockProtocol{id: "ga4"}, batch)
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
			first.EventName, first.UserID, first.Metadata = "old-name", &uid, nil
			second := hits.NewWithServerAttributes(request)
			config := transformationConfig("true", transformationSet("name", "new-name"),
				transformationSet("user_id", "new-user"), transformationSet("page_title", "new-title"))
			registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{
				Transformations: config,
			}))
			// when
			retained, err := NewTransformationBatchProcessingRule(registry, transformations.ExtractionDependencies{})(
				&mockProtocol{id: protocolID}, []*hits.Hit{first, second},
			)
			// then
			require.NoError(t, err)
			require.Len(t, retained, 2)
			for _, hit := range retained {
				assert.Equal(t, "new-name", hit.EventName)
				assert.Equal(t, hit.EventName, hit.Request.QueryParams.Get("en"))
				require.NotNil(t, hit.UserID)
				assert.Equal(t, *hit.UserID, hit.Request.QueryParams.Get("uid"))
				assert.Nil(t, hit.Request.Body)
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
			assert.Nil(t, first.Metadata)
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
	})), WithHitBatchProcessingRule(func(_ protocol.Protocol, batch []*hits.Hit) ([]*hits.Hit, error) {
		order = append(order, "batch")
		for _, hit := range batch {
			assert.Equal(t, "192.0.2.0", hit.Request.IP)
			hit.EventName = "transformed"
		}
		return batch[1:], nil
	}))
	// when
	retained, err := s.createHits(&fasthttp.RequestCtx{}, p)
	// then
	require.NoError(t, err)
	assert.Len(t, retained, 1)
	assert.Equal(t, []string{"process", "process", "batch", "validate:transformed"}, order)
	assert.Len(t, raw.requests, 1)
}

func TestTransformationBatchFailuresArePayloadFree(t *testing.T) {
	for _, tc := range []struct {
		name     string
		registry properties.SettingsRegistry
		config   *properties.TransformationsConfig
		deps     transformations.ExtractionDependencies
	}{
		{name: "missing registry"},
		{name: "settings lookup", registry: batchSettingsRegistry{get: func(string) (*properties.Settings, error) {
			return nil, errors.New("secret-canary")
		}}},
		{name: "compile", config: transformationConfig(`unknown_secret_canary`,
			properties.TransformationActionConfig{Type: "drop"})},
		{name: "evaluation", config: transformationConfig(`split(name, "")[99] == "secret-canary"`,
			properties.TransformationActionConfig{Type: "drop"})},
		{name: "extraction", config: transformationConfig(`geo_country == "secret-canary"`,
			properties.TransformationActionConfig{Type: "drop"}), deps: transformations.ExtractionDependencies{
			Geo: dbip.NewStaticLookupProvider(nil, errors.New("secret-canary")),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			registry := tc.registry
			if tc.config != nil {
				registry = properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{
					Transformations: tc.config,
				}))
			}
			hit := hits.New()
			hit.EventName, hit.Request.IP = "x", "192.0.2.0"
			// when
			retained, err := NewTransformationBatchProcessingRule(registry, tc.deps)(
				&mockProtocol{id: "ga4"}, []*hits.Hit{hit},
			)
			// then
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "secret")
			assert.Nil(t, retained)
		})
	}
}
