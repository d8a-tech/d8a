package receiver

import (
	"context"
	"fmt"
	"testing"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

// mockStorage implements Storage interface for testing
type mockStorage struct {
	hits []*hits.Hit
	err  error
}

func (m *mockStorage) Push(hits []*hits.Hit) error {
	m.hits = hits
	return m.err
}

// mockProtocol implements Protocol interface for testing
type mockProtocol struct {
	id      string
	columns schema.Columns
	err     error
	hits    []*hits.Hit
}

func (m *mockProtocol) ID() string {
	return m.id
}

func (m *mockProtocol) Interfaces() any {
	return struct{}{}
}

func (m *mockProtocol) Endpoints() []protocol.ProtocolEndpoint {
	return []protocol.ProtocolEndpoint{
		{
			Methods: []string{fasthttp.MethodPost},
			Path:    "/collect",
		},
	}
}

func (m *mockProtocol) Hits(_ *fasthttp.RequestCtx, request *hits.ParsedRequest) ([]*hits.Hit, error) {
	if m.hits != nil {
		for _, hit := range m.hits {
			hit.Request = request.Clone()
		}
		return m.hits, m.err
	}

	theHit := hits.New()

	theHit.ClientID = hits.ClientID("test_client_id")
	theHit.AuthoritativeClientID = theHit.ClientID
	theHit.PropertyID = "test_property_id"
	theHit.EventName = "page_view"
	theHit.Request = request.Clone()
	return []*hits.Hit{theHit}, m.err
}

func (m *mockProtocol) Columns() schema.Columns {
	return m.columns
}

type capturingRawLogStorage struct {
	requests []*hits.ParsedRequest
}

func (c *capturingRawLogStorage) Store(request *hits.ParsedRequest) error {
	c.requests = append(c.requests, request.Clone())
	return nil
}

type settingsRegistryStub struct {
	settingsByPropertyID map[string]*properties.Settings
	err                  error
}

func (s settingsRegistryStub) GetByMeasurementID(string) (*properties.Settings, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s settingsRegistryStub) GetByPropertyID(propertyID string) (*properties.Settings, error) {
	if s.err != nil {
		return nil, s.err
	}

	settings, ok := s.settingsByPropertyID[propertyID]
	if !ok {
		return nil, fmt.Errorf("unknown property ID: %s", propertyID)
	}

	return settings, nil
}

func TestHandleRequest(t *testing.T) {
	tests := []struct {
		name           string
		request        func() *fasthttp.RequestCtx
		protocols      []protocol.Protocol
		storageErr     error
		expectedStatus int
		validateHit    func(*testing.T, *hits.Hit)
	}{
		{
			name: "successful request mock protocol",
			request: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.SetHost("example.com")
				ctx.Request.Header.SetHost("example.com")
				ctx.Request.Header.Set("X-Real-IP", "192.168.1.1")
				ctx.URI().SetQueryString("param1=value1&param2=value2")
				ctx.Request.Header.Set("User-Agent", "test-agent")
				ctx.URI().SetPath("/collect")
				return ctx
			},
			protocols: []protocol.Protocol{
				&mockProtocol{id: "test_protocol"},
			},
			storageErr:     nil,
			expectedStatus: fasthttp.StatusNoContent,
			validateHit: func(t *testing.T, hit *hits.Hit) {
				assert.Equal(t, "192.168.1.1", hit.Request.IP)
				assert.Equal(t, "example.com", hit.Request.Host)
				assert.Equal(t, "/collect", hit.Request.Path)
				assert.Equal(t, "GET", hit.Request.Method)
				assert.Equal(t, []string{"value1"}, hit.Request.QueryParams["param1"])
				assert.Equal(t, []string{"value2"}, hit.Request.QueryParams["param2"])
				assert.Equal(t, []string{"test-agent"}, hit.Request.Headers["User-Agent"])
				assert.Equal(t, "test_client_id", string(hit.ClientID))
				assert.Equal(t, "test_protocol", hit.Metadata[HitProtocolMetadataKey])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			storage := &mockStorage{err: tt.storageErr}
			ctx := tt.request()
			server := NewServer(
				storage,
				NewDummyRawLogStorage(),
				HitValidatingRuleSet(1024*128, properties.NewStaticSettingsRegistry([]properties.Settings{
					{
						PropertyID: "test_property_id",
						ProtocolID: "test_protocol",
					},
				})),
				tt.protocols,
				8080,
				WithTrustAllProxies(),
			)

			// when
			server.handleRequest(context.Background(), ctx, tt.protocols[0])

			// then
			fmt.Println(string(ctx.Response.Body()))
			assert.Equal(t, tt.expectedStatus, ctx.Response.StatusCode())
			if tt.validateHit != nil && len(storage.hits) > 0 {
				tt.validateHit(t, storage.hits[0])
			}
		})
	}
}

func TestHandleRequest_ErrorResponsesDoNotLeakInternalDetails(t *testing.T) {
	// sensitiveStrings are patterns that must never appear in HTTP response bodies.
	sensitiveStrings := []string{
		"property",
		"protocol",
		"hit.",
		"nil",
		"storage",
		"bolt",
		"test_property_id",
		"test_protocol",
		"disk full",
	}

	assertNoLeaks := func(t *testing.T, body string) {
		t.Helper()
		for _, s := range sensitiveStrings {
			assert.NotContains(t, body, s,
				"response body should not contain internal detail %q", s)
		}
	}

	defaultRequest := func() *fasthttp.RequestCtx {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.SetHost("example.com")
		ctx.Request.Header.SetHost("example.com")
		ctx.Request.Header.Set("X-Real-IP", "192.168.1.1")
		ctx.URI().SetQueryString("param1=value1")
		ctx.Request.Header.Set("User-Agent", "test-agent")
		ctx.URI().SetPath("/collect")
		return ctx
	}

	tests := []struct {
		name           string
		protocolErr    error
		storageErr     error
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "protocol error does not leak details",
			protocolErr:    fmt.Errorf("property test_property_id not found in protocol registry"),
			storageErr:     nil,
			expectedStatus: fasthttp.StatusBadRequest,
			expectedBody:   "Bad Request",
		},
		{
			name:           "storage error does not leak details",
			protocolErr:    nil,
			storageErr:     fmt.Errorf("bolt: disk full, cannot write to storage"),
			expectedStatus: fasthttp.StatusInternalServerError,
			expectedBody:   "Internal Server Error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			storage := &mockStorage{err: tt.storageErr}
			p := &mockProtocol{id: "test_protocol", err: tt.protocolErr}
			server := NewServer(
				storage,
				NewDummyRawLogStorage(),
				HitValidatingRuleSet(1024*128, properties.NewStaticSettingsRegistry([]properties.Settings{
					{
						PropertyID: "test_property_id",
						ProtocolID: "test_protocol",
					},
				})),
				[]protocol.Protocol{p},
				8080,
				WithTrustAllProxies(),
			)

			ctx := defaultRequest()

			// when
			server.handleRequest(context.Background(), ctx, p)

			// then
			assert.Equal(t, tt.expectedStatus, ctx.Response.StatusCode())
			body := string(ctx.Response.Body())
			assert.Contains(t, body, tt.expectedBody)
			assertNoLeaks(t, body)
		})
	}
}

func TestHandleRequest_ProtocolMismatchReturnsSafeDetails(t *testing.T) {
	// given — use a protocol whose property ID doesn't match settings,
	// triggering PropertyProtocolMatchesTheEndpointProtocol validation error.
	storage := &mockStorage{}
	p := &mockProtocol{id: "wrong_protocol"}
	server := NewServer(
		storage,
		NewDummyRawLogStorage(),
		HitValidatingRuleSet(1024*128, properties.NewStaticSettingsRegistry([]properties.Settings{
			{
				PropertyID: "test_property_id",
				ProtocolID: "test_protocol",
			},
		})),
		[]protocol.Protocol{p},
		8080,
		WithTrustAllProxies(),
	)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetHost("example.com")
	ctx.Request.Header.SetHost("example.com")
	ctx.Request.Header.Set("X-Real-IP", "192.168.1.1")
	ctx.URI().SetQueryString("param1=value1")
	ctx.Request.Header.Set("User-Agent", "test-agent")
	ctx.URI().SetPath("/collect")

	// when
	server.handleRequest(context.Background(), ctx, p)

	// then
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	body := string(ctx.Response.Body())
	assert.Contains(t, body, "property protocol test_protocol does not match endpoint protocol wrong_protocol")
	assert.NotContains(t, body, "test_property_id")
}

func TestHandleRequest_SettingsErrorDoesNotLeakDetails(t *testing.T) {
	// given
	storage := &mockStorage{}
	p := &mockProtocol{id: "test_protocol"}
	server := NewServer(
		storage,
		NewDummyRawLogStorage(),
		HitValidatingRuleSet(1024*128, settingsRegistryStub{err: fmt.Errorf("settings database unavailable")}),
		[]protocol.Protocol{p},
		8080,
		WithTrustAllProxies(),
	)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetHost("example.com")
	ctx.Request.Header.SetHost("example.com")
	ctx.Request.Header.Set("X-Real-IP", "192.168.1.1")
	ctx.Request.Header.Set("User-Agent", "test-agent")
	ctx.URI().SetPath("/collect")

	// when
	server.handleRequest(context.Background(), ctx, p)

	// then
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	assert.Equal(t, "Bad Request", string(ctx.Response.Body()))
}

func TestHandleRequest_MasksIPBeforeStorageAndRawLog(t *testing.T) {
	// given
	storage := &mockStorage{}
	rawLogStorage := &capturingRawLogStorage{}
	settingsRegistry := settingsRegistryStub{
		settingsByPropertyID: map[string]*properties.Settings{
			"test_property_id": {
				PropertyID:     "test_property_id",
				ProtocolID:     "test_protocol",
				IPMaskingLevel: 1,
			},
		},
	}
	p := &mockProtocol{id: "test_protocol"}
	server := NewServer(
		storage,
		rawLogStorage,
		HitValidatingRuleSet(1024*128, settingsRegistry),
		[]protocol.Protocol{p},
		8080,
		WithTrustAllProxies(),
		WithHitProcessingRule(IPMasking(settingsRegistry)),
	)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetHost("example.com")
	ctx.Request.Header.SetHost("example.com")
	ctx.Request.Header.Set("X-Real-IP", "192.168.1.123")
	ctx.URI().SetPath("/collect")

	// when
	server.handleRequest(context.Background(), ctx, p)

	// then
	assert.Equal(t, fasthttp.StatusNoContent, ctx.Response.StatusCode())
	assert.Len(t, storage.hits, 1)
	assert.Equal(t, "192.168.1.0", storage.hits[0].MustParsedRequest().IP)
	assert.Len(t, rawLogStorage.requests, 1)
	assert.Equal(t, "192.168.1.0", rawLogStorage.requests[0].IP)
}

func TestHandleRequest_IPMaskingRegistryErrorReturnsBadRequest(t *testing.T) {
	// given
	storage := &mockStorage{}
	settingsRegistry := settingsRegistryStub{err: fmt.Errorf("registry unavailable")}
	p := &mockProtocol{id: "test_protocol"}
	server := NewServer(
		storage,
		NewDummyRawLogStorage(),
		HitValidatingRuleSet(1024*128, properties.NewStaticSettingsRegistry([]properties.Settings{{
			PropertyID: "test_property_id",
			ProtocolID: "test_protocol",
		}})),
		[]protocol.Protocol{p},
		8080,
		WithTrustAllProxies(),
		WithHitProcessingRule(IPMasking(settingsRegistry)),
	)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetHost("example.com")
	ctx.Request.Header.SetHost("example.com")
	ctx.Request.Header.Set("X-Real-IP", "192.168.1.123")
	ctx.URI().SetPath("/collect")

	// when
	server.handleRequest(context.Background(), ctx, p)

	// then
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	assert.Empty(t, storage.hits)
	assert.Contains(t, string(ctx.Response.Body()), "Bad Request")
}

func TestHandleRequest_StoresRawLogOnceAfterAllHitsValidate(t *testing.T) {
	// given
	storage := &mockStorage{}
	rawLogStorage := &capturingRawLogStorage{}
	settingsRegistry := settingsRegistryStub{
		settingsByPropertyID: map[string]*properties.Settings{
			"test_property_id": {
				PropertyID:     "test_property_id",
				ProtocolID:     "test_protocol",
				IPMaskingLevel: 1,
			},
		},
	}
	hitOne := hits.New()
	hitOne.PropertyID = "test_property_id"
	hitOne.ClientID = "one"
	hitOne.AuthoritativeClientID = hitOne.ClientID
	hitOne.EventName = "page_view"
	hitTwo := hits.New()
	hitTwo.PropertyID = "test_property_id"
	hitTwo.ClientID = "two"
	hitTwo.AuthoritativeClientID = hitTwo.ClientID
	hitTwo.EventName = "page_view"
	p := &mockProtocol{id: "test_protocol", hits: []*hits.Hit{hitOne, hitTwo}}
	server := NewServer(
		storage,
		rawLogStorage,
		HitValidatingRuleSet(1024*128, settingsRegistry),
		[]protocol.Protocol{p},
		8080,
		WithTrustAllProxies(),
		WithHitProcessingRule(IPMasking(settingsRegistry)),
	)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetHost("example.com")
	ctx.Request.Header.SetHost("example.com")
	ctx.Request.Header.Set("X-Real-IP", "192.168.1.123")
	ctx.URI().SetPath("/collect")

	// when
	server.handleRequest(context.Background(), ctx, p)

	// then
	assert.Equal(t, fasthttp.StatusNoContent, ctx.Response.StatusCode())
	assert.Len(t, storage.hits, 2)
	assert.Len(t, rawLogStorage.requests, 1)
	assert.Equal(t, "192.168.1.0", rawLogStorage.requests[0].IP)
}

func TestHandleRequest_DoesNotStoreRawLogWhenLaterHitValidationFails(t *testing.T) {
	// given
	storage := &mockStorage{}
	rawLogStorage := &capturingRawLogStorage{}
	settingsRegistry := settingsRegistryStub{
		settingsByPropertyID: map[string]*properties.Settings{
			"test_property_id": {
				PropertyID: "test_property_id",
				ProtocolID: "test_protocol",
			},
			"bad_property_id": {
				PropertyID: "bad_property_id",
				ProtocolID: "different_protocol",
			},
		},
	}
	hitOne := hits.New()
	hitOne.PropertyID = "test_property_id"
	hitOne.ClientID = "one"
	hitOne.AuthoritativeClientID = hitOne.ClientID
	hitOne.EventName = "page_view"
	hitTwo := hits.New()
	hitTwo.PropertyID = "bad_property_id"
	hitTwo.ClientID = "two"
	hitTwo.AuthoritativeClientID = hitTwo.ClientID
	hitTwo.EventName = "page_view"
	p := &mockProtocol{id: "test_protocol", hits: []*hits.Hit{hitOne, hitTwo}}
	server := NewServer(
		storage,
		rawLogStorage,
		HitValidatingRuleSet(1024*128, settingsRegistry),
		[]protocol.Protocol{p},
		8080,
		WithTrustAllProxies(),
		WithHitProcessingRule(IPMasking(settingsRegistry)),
	)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetHost("example.com")
	ctx.Request.Header.SetHost("example.com")
	ctx.Request.Header.Set("X-Real-IP", "192.168.1.123")
	ctx.URI().SetPath("/collect")

	// when
	server.handleRequest(context.Background(), ctx, p)

	// then
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	assert.Empty(t, storage.hits)
	assert.Empty(t, rawLogStorage.requests)
}

func TestHandleRequest_StoresRawLogOnceForSuccessfulZeroHitRequest(t *testing.T) {
	// given
	storage := &mockStorage{}
	rawLogStorage := &capturingRawLogStorage{}
	p := &mockProtocol{id: "test_protocol", hits: []*hits.Hit{}}
	server := NewServer(
		storage,
		rawLogStorage,
		HitValidatingRuleSet(1024*128, properties.NewStaticSettingsRegistry([]properties.Settings{{
			PropertyID: "test_property_id",
			ProtocolID: "test_protocol",
		}})),
		[]protocol.Protocol{p},
		8080,
		WithTrustAllProxies(),
	)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetHost("example.com")
	ctx.Request.Header.SetHost("example.com")
	ctx.Request.Header.Set("X-Real-IP", "192.168.1.123")
	ctx.URI().SetPath("/collect")

	// when
	server.handleRequest(context.Background(), ctx, p)

	// then
	assert.Equal(t, fasthttp.StatusNoContent, ctx.Response.StatusCode())
	assert.Empty(t, storage.hits)
	assert.Len(t, rawLogStorage.requests, 1)
	assert.Equal(t, "192.168.1.123", rawLogStorage.requests[0].IP)
}
