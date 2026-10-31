package receiver

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	native "github.com/d8a-tech/d8a/pkg/protocol/d8a"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/protocol/matomo"
	"github.com/d8a-tech/d8a/pkg/transformations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

type normalizedStorage struct {
	batch []*hits.Hit
	calls int
}

func (s *normalizedStorage) Push(batch []*hits.Hit) error {
	s.calls++
	s.batch = batch
	return nil
}

type normalizedRawLog struct {
	requests []*hits.ParsedRequest
	err      error
}

func (r *normalizedRawLog) Store(request *hits.ParsedRequest) error {
	r.requests = append(r.requests, request)
	return r.err
}

func normalizationProtocol(id string, registry properties.SettingsRegistry) protocol.Protocol {
	switch id {
	case "ga4":
		return ga4.NewGA4Protocol(nil, registry)
	case "d8a":
		return native.NewD8AProtocol(nil, registry)
	default:
		return matomo.NewMatomoProtocol(matomo.NewFromIDSiteExtractor(registry), registry)
	}
}

func normalizationRequest(t *testing.T, id string, titles []string) *fasthttp.RequestCtx {
	t.Helper()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("http://example.test/collect?tid=measurement&cid=client")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.Header.Set("User-Agent", "test-agent")
	lines := make([]string, 0, len(titles))
	for _, title := range titles {
		params := url.Values{"en": {"page_view"}, "dt": {title}, "uid": {"old"}, "dl": {"https://example.test/"}}
		if id == "matomo" {
			params = url.Values{"idsite": {"measurement"}, "action_name": {title}, "url": {"https://example.test/"}}
		}
		lines = append(lines, params.Encode())
	}
	body := strings.Join(lines, "\n")
	if id == "matomo" {
		encoded, err := json.Marshal(map[string][]string{"requests": lines})
		require.NoError(t, err)
		body = string(encoded)
	}
	ctx.Request.SetBodyString(body)
	return ctx
}

func TestNormalizedRawLogsDropPositions(t *testing.T) {
	for _, id := range []string{"ga4", "d8a", "matomo"} {
		for _, tc := range []struct {
			name   string
			titles []string
			want   int
		}{
			{"first", []string{"DROPPED_CANARY", "second", "third"}, 2},
			{"middle", []string{"first", "DROPPED_CANARY", "third"}, 2},
			{"last", []string{"first", "second", "DROPPED_CANARY"}, 2},
			{"all", []string{"DROPPED_CANARY", "DROPPED_CANARY", "DROPPED_CANARY"}, 0},
		} {
			t.Run(id+"/"+tc.name, func(t *testing.T) {
				// given
				config := transformationConfig(`page_title == "DROPPED_CANARY"`,
					properties.TransformationActionConfig{Type: "drop"})
				config.Rules = append(config.Rules, transformationConfig("true",
					transformationSet("page_title", "normalized"), transformationSet("user_id", "transformed-user"),
					properties.TransformationActionConfig{Type: "copy",
						Target: &properties.TransformationFieldReference{Field: "page_referrer"},
						Source: &properties.TransformationFieldReference{Field: "page_title"}},
					properties.TransformationActionConfig{Type: "remove",
						Target: &properties.TransformationFieldReference{Field: "page_location"}},
				).Rules...)
				registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{
					PropertyID: "property", ProtocolID: id, Transformations: config,
				}))
				queue, raw := &normalizedStorage{}, &normalizedRawLog{}
				p := normalizationProtocol(id, registry)
				ctx := normalizationRequest(t, id, tc.titles)
				s := NewServer(queue, raw, HitValidatingRuleSet(1024*1024, registry), nil, 0,
					WithHitBatchProcessingRule(NewTransformationBatchProcessingRule(
						registry, transformations.ExtractionDependencies{},
					)))
				// when
				s.handleRequest(context.Background(), ctx, p)
				// then
				require.Equal(t, fasthttp.StatusNoContent, ctx.Response.StatusCode(), string(ctx.Response.Body()))
				assert.Len(t, queue.batch, tc.want)
				assert.Len(t, raw.requests, tc.want)
				assert.Equal(t, min(tc.want, 1), queue.calls)
				queued, err := json.Marshal(queue.batch)
				require.NoError(t, err)
				logged, err := json.Marshal(raw.requests)
				require.NoError(t, err)
				assert.NotContains(t, string(queued), "DROPPED_CANARY")
				assert.NotContains(t, string(queued), normalizedRequestMetadataKey)
				assert.NotContains(t, string(logged), "DROPPED_CANARY")
				for i, request := range raw.requests {
					assert.Nil(t, request.Body)
					assert.Nil(t, queue.batch[i].Request.Body)
					assert.Equal(t, "example.test", request.Host)
					assert.Equal(t, "test-agent", request.Headers.Get("User-Agent"))
					assert.Equal(t, "transformed-user", request.QueryParams.Get("uid"))
					require.NotNil(t, queue.batch[i].UserID)
					assert.Equal(t, "transformed-user", *queue.batch[i].UserID)
					assert.Equal(t, request.QueryParams, queue.batch[i].Request.QueryParams)
					titleKey := map[string]string{"ga4": "dt", "d8a": "dt", "matomo": "action_name"}[id]
					assert.Equal(t, "normalized", request.QueryParams.Get(titleKey))
					referrerKey := map[string]string{"ga4": "dr", "d8a": "dr", "matomo": "urlref"}[id]
					assert.Contains(t, tc.titles, request.QueryParams.Get(referrerKey))
					locationKey := map[string]string{"ga4": "dl", "d8a": "dl", "matomo": "url"}[id]
					assert.NotContains(t, request.QueryParams, locationKey)
				}
			})
		}
	}
}

func TestMixedPropertyBatchNormalization(t *testing.T) {
	// given
	registry := properties.NewStaticSettingsRegistry([]properties.Settings{
		{PropertyID: "configured", PropertyMeasurementID: "enabled", Transformations: transformationConfig(
			"true", properties.TransformationActionConfig{Type: "drop"})},
		{PropertyID: "legacy", PropertyMeasurementID: "disabled"},
	})
	ctx := normalizationRequest(t, "ga4", []string{"unused"})
	ctx.Request.SetBodyString("tid=enabled&en=DROPPED_CANARY\ntid=disabled&en=retained")
	queue, raw := &normalizedStorage{}, &normalizedRawLog{}
	s := NewServer(queue, raw, EventNameNotEmpty, nil, 0,
		WithHitBatchProcessingRule(NewTransformationBatchProcessingRule(registry, transformations.ExtractionDependencies{})))
	// when
	s.handleRequest(context.Background(), ctx, normalizationProtocol("ga4", registry))
	// then
	require.Equal(t, fasthttp.StatusNoContent, ctx.Response.StatusCode())
	require.Len(t, queue.batch, 1)
	require.Len(t, raw.requests, 1)
	assert.Equal(t, "legacy", queue.batch[0].PropertyID)
	assert.Nil(t, queue.batch[0].Request.Body)
	assert.Nil(t, raw.requests[0].Body)
	assert.NotContains(t, raw.requests[0].QueryParams.Encode(), "DROPPED_CANARY")
}

func TestTransformationPersistenceFailures(t *testing.T) {
	for _, tc := range []struct {
		name          string
		config        *properties.TransformationsConfig
		settingsErr   error
		deps          transformations.ExtractionDependencies
		validationErr error
		rawErr        error
		wantStatus    int
		wantWrites    int
	}{
		{name: "settings", settingsErr: errors.New("secret-canary"), wantStatus: 400},
		{name: "compile", config: transformationConfig("unknown_field",
			properties.TransformationActionConfig{Type: "drop"}), wantStatus: 400},
		{name: "evaluation", config: transformationConfig(`split(name, "")[99] == "x"`,
			properties.TransformationActionConfig{Type: "drop"}), wantStatus: 400},
		{name: "extraction", config: transformationConfig(`geo_country == "NL"`,
			properties.TransformationActionConfig{Type: "drop"}), deps: transformations.ExtractionDependencies{
			Geo: dbip.NewStaticLookupProvider(nil, errors.New("secret-canary")),
		}, wantStatus: 400},
		{name: "validation", validationErr: errors.New("secret-canary"), wantStatus: 400},
		{name: "raw log nonfatal", rawErr: errors.New("secret-canary"), wantStatus: 204, wantWrites: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			config := tc.config
			if config == nil {
				config = transformationConfig("true", transformationSet("page_title", "normalized"))
			}
			parseRegistry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{
				PropertyID: "property",
			}))
			registry := batchSettingsRegistry{get: func(string) (*properties.Settings, error) {
				return &properties.Settings{Transformations: config}, tc.settingsErr
			}}
			queue, raw := &normalizedStorage{}, &normalizedRawLog{err: tc.rawErr}
			ctx := normalizationRequest(t, "ga4", []string{"first", "second"})
			validationCount := 0
			s := NewServer(queue, raw, NewSimpleHitValidatingRule(func(_ protocol.Protocol, _ *hits.Hit) error {
				validationCount++
				if validationCount == 2 {
					return tc.validationErr
				}
				return nil
			}), nil, 0, WithHitProcessingRule(NewSimpleHitProcessingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
				hit.Request.IP = "192.0.2.0"
				return nil
			})), WithHitBatchProcessingRule(NewTransformationBatchProcessingRule(registry, tc.deps)))
			// when
			s.handleRequest(context.Background(), ctx, normalizationProtocol("ga4", parseRegistry))
			// then
			assert.Equal(t, tc.wantStatus, ctx.Response.StatusCode())
			assert.NotContains(t, string(ctx.Response.Body()), "secret-canary")
			assert.Len(t, raw.requests, tc.wantWrites)
			assert.Len(t, queue.batch, tc.wantWrites)
			assert.Equal(t, min(tc.wantWrites, 1), queue.calls)
		})
	}
}

func TestEmptyTransformationsPreserveLegacyRawLog(t *testing.T) {
	for _, config := range []*properties.TransformationsConfig{nil, {}} {
		t.Run("empty", func(t *testing.T) {
			// given
			registry := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{
				PropertyID: "property", Transformations: config,
			}))
			queue, raw := &normalizedStorage{}, &normalizedRawLog{}
			ctx := normalizationRequest(t, "ga4", []string{"first", "second"})
			original := string(ctx.Request.Body())
			s := NewServer(queue, raw, EventNameNotEmpty, nil, 0,
				WithHitBatchProcessingRule(NewTransformationBatchProcessingRule(
					registry, transformations.ExtractionDependencies{},
				)))
			// when
			s.handleRequest(context.Background(), ctx, normalizationProtocol("ga4", registry))
			// then
			require.Equal(t, fasthttp.StatusNoContent, ctx.Response.StatusCode())
			require.Len(t, raw.requests, 1)
			assert.Equal(t, original, string(raw.requests[0].Body))
			require.Len(t, queue.batch, 2)
			assert.Equal(t, original, string(queue.batch[1].Request.Body))
		})
	}
}

func TestZeroParsedHitsRetainsLegacyRawLogFallback(t *testing.T) {
	// given
	queue, raw := &normalizedStorage{}, &normalizedRawLog{}
	ctx := normalizationRequest(t, "ga4", []string{"original"})
	p := &mockProtocol{id: "ga4", hits: []*hits.Hit{}}
	s := NewServer(queue, raw, EventNameNotEmpty, nil, 0)
	// when
	s.handleRequest(context.Background(), ctx, p)
	// then
	assert.Equal(t, fasthttp.StatusNoContent, ctx.Response.StatusCode())
	assert.Zero(t, queue.calls)
	require.Len(t, raw.requests, 1)
	assert.Equal(t, string(ctx.Request.Body()), string(raw.requests[0].Body))
}
