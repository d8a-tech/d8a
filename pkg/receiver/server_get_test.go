package receiver

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	fasthttputil "github.com/valyala/fasthttp/fasthttputil"
)

// captureStorage implements Storage and captures pushed hits for testing
type captureStorage struct {
	hits []*hits.Hit
}

func (c *captureStorage) Push(h []*hits.Hit) error {
	c.hits = h
	return nil
}

// TestGA4GetRequestHitExtraction tests that GET requests to GA4 /g/collect endpoint
// extract hits correctly from query parameters.
func TestGA4GetRequestHitExtraction(t *testing.T) {
	// given
	testPropertyID := "test_property_id"
	testMeasurementID := "G-TEST123"
	testClientID := "client_123.456"
	testEventName := "page_view"
	testUserID := "user_789"

	// Create a custom property ID extractor that returns our test property ID
	extractor := &staticPropertyIDExtractor{propertyID: testPropertyID}

	ga4Protocol := ga4.NewGA4Protocol(
		currency.NewDummyConverter(1),
		properties.NewTestSettingRegistry(),
		ga4.WithPropertyIDExtractor(extractor),
	)

	storage := &captureStorage{}
	settingsRegistry := properties.NewStaticSettingsRegistry(
		[]properties.Settings{
			{
				PropertyID:   testPropertyID,
				PropertyName: "Test Property",
				ProtocolID:   "ga4",
			},
		},
	)

	server := NewServer(
		storage,
		NewDummyRawLogStorage(),
		HitValidatingRuleSet(1024*128, settingsRegistry),
		[]protocol.Protocol{ga4Protocol},
		9999,
	)

	// Create an in-memory fasthttp listener
	ln := fasthttputil.NewInmemoryListener()
	defer func() {
		if err := ln.Close(); err != nil {
			t.Logf("Failed to close listener: %v", err)
		}
	}()

	// Create fasthttp server with router
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fastHTTPServer := &fasthttp.Server{
		Handler: server.setupRouter(ctx).Handler,
	}

	// Serve in background
	go func() {
		err := fastHTTPServer.Serve(ln)
		if err != nil && fmt.Sprintf("%v", err) != "accept tcp: use of closed network connection" {
			t.Logf("Server error: %v", err)
		}
	}()

	// when
	// Build GET request URL with query parameters
	client := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(fmt.Sprintf(
		"http://localhost/g/collect?v=2&tid=%s&cid=%s&en=%s&uid=%s",
		testMeasurementID, testClientID, testEventName, testUserID,
	))
	req.Header.SetMethod(fasthttp.MethodGet)
	req.Header.Set("User-Agent", "test-agent")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	err := client.Do(req, resp)
	cancel()

	// then
	require.NoError(t, err)
	assert.Equal(t, fasthttp.StatusNoContent, resp.StatusCode(), "Expected 204 No Content response")
	assert.Len(t, storage.hits, 1, "Expected exactly one hit to be stored")

	hit := storage.hits[0]
	assert.Equal(t, testClientID, string(hit.ClientID), "ClientID should match query param")
	assert.Equal(t, testEventName, hit.EventName, "EventName should match query param")
	assert.Equal(t, testUserID, *hit.UserID, "UserID should match query param")
	assert.Equal(t, testPropertyID, hit.PropertyID, "PropertyID should be set by extractor")
	assert.Equal(t, "GET", hit.Request.Method, "Request method should be GET")
	assert.Empty(t, hit.Request.Body, "GET request should have empty body")
	assert.Equal(t, testMeasurementID, hit.Request.QueryParams.Get("tid"), "tid query param should be preserved")
	assert.Equal(t, testClientID, hit.Request.QueryParams.Get("cid"), "cid query param should be preserved")
	assert.Equal(t, testEventName, hit.Request.QueryParams.Get("en"), "en query param should be preserved")
	assert.Equal(t, testUserID, hit.Request.QueryParams.Get("uid"), "uid query param should be preserved")
	assert.NotEmpty(t, hit.Request.Headers.Get("User-Agent"), "User-Agent header should be captured")
}

// staticPropertyIDExtractor is a test helper that always returns a fixed property ID
type staticPropertyIDExtractor struct {
	propertyID string
}

func (e *staticPropertyIDExtractor) PropertyID(ctx *protocol.RequestContext) (string, error) {
	return e.propertyID, nil
}
