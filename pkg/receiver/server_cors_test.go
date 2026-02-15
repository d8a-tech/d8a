package receiver

import (
	"fmt"
	"testing"

	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/d8a"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

type staticPropertyIDExtractorCORS struct {
	propertyID string
}

func (e *staticPropertyIDExtractorCORS) PropertyID(_ *protocol.RequestContext) (string, error) {
	return e.propertyID, nil
}

type captureStorageCORS struct {
	hits []*hits.Hit
}

func (c *captureStorageCORS) Push(h []*hits.Hit) error {
	c.hits = h
	return nil
}

func TestCORS_GA4_Collect_Preflight_MirrorsOriginAndCredentials(t *testing.T) {
	// given
	storage := &captureStorageCORS{}
	settingsRegistry := buildSettingsRegistry("ga4", "test_property_id", "Test")
	ga4Protocol := ga4.NewGA4Protocol(
		currency.NewDummyConverter(1),
		properties.NewTestSettingRegistry(),
		ga4.WithPropertyIDExtractor(&staticPropertyIDExtractorCORS{propertyID: "test_property_id"}),
	)
	server := NewServer(
		storage,
		NewDummyRawLogStorage(),
		HitValidatingRuleSet(1024*128, settingsRegistry),
		[]protocol.Protocol{ga4Protocol},
		9999,
	)
	r := newInmemReceiver(t, server)

	req := fasthttp.AcquireRequest()
	t.Cleanup(func() { fasthttp.ReleaseRequest(req) })
	req.SetRequestURI("http://localhost/g/collect")
	req.Header.SetMethod(fasthttp.MethodOptions)
	req.Header.Set("Origin", "https://example.test")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "content-type,x-requested-with")

	// when
	resp := r.do(req)

	// then
	assert.Equal(t, fasthttp.StatusNoContent, resp.StatusCode())
	assert.Equal(t, "https://example.test", string(resp.Header.Peek("Access-Control-Allow-Origin")))
	assert.Equal(t, "true", string(resp.Header.Peek("Access-Control-Allow-Credentials")))
	assert.Contains(t, string(resp.Header.Peek("Vary")), "Origin")
	assert.Contains(t, string(resp.Header.Peek("Access-Control-Allow-Methods")), "OPTIONS")
	assert.Contains(t, string(resp.Header.Peek("Access-Control-Allow-Methods")), "POST")
	assert.Equal(t, "content-type,x-requested-with", string(resp.Header.Peek("Access-Control-Allow-Headers")))
}

func TestCORS_GA4_Collect_GET_FallbacksToWildcardWithoutOrigin(t *testing.T) {
	// given
	storage := &captureStorageCORS{}
	settingsRegistry := buildSettingsRegistry("ga4", "test_property_id", "Test")
	ga4Protocol := ga4.NewGA4Protocol(
		currency.NewDummyConverter(1),
		properties.NewTestSettingRegistry(),
		ga4.WithPropertyIDExtractor(&staticPropertyIDExtractorCORS{propertyID: "test_property_id"}),
	)
	server := NewServer(
		storage,
		NewDummyRawLogStorage(),
		HitValidatingRuleSet(1024*128, settingsRegistry),
		[]protocol.Protocol{ga4Protocol},
		9999,
	)
	r := newInmemReceiver(t, server)

	req := fasthttp.AcquireRequest()
	t.Cleanup(func() { fasthttp.ReleaseRequest(req) })
	req.SetRequestURI(fmt.Sprintf(
		"http://localhost/g/collect?v=2&tid=%s&cid=%s&en=%s",
		"G-TEST123", "client_123.456", "page_view",
	))
	req.Header.SetMethod(fasthttp.MethodGet)

	// when
	resp := r.do(req)

	// then
	assert.Equal(t, fasthttp.StatusNoContent, resp.StatusCode())
	assert.Equal(t, "*", string(resp.Header.Peek("Access-Control-Allow-Origin")))
	assert.Empty(t, string(resp.Header.Peek("Access-Control-Allow-Credentials")))
}

func TestCORS_GA4_Static_Preflight_UsesWildcard(t *testing.T) {
	// given
	storage := &captureStorageCORS{}
	settingsRegistry := buildSettingsRegistry("ga4", "test_property_id", "Test")
	ga4Protocol := ga4.NewGA4Protocol(
		currency.NewDummyConverter(1),
		properties.NewTestSettingRegistry(),
		ga4.WithPropertyIDExtractor(&staticPropertyIDExtractorCORS{propertyID: "test_property_id"}),
	)
	server := NewServer(
		storage,
		NewDummyRawLogStorage(),
		HitValidatingRuleSet(1024*128, settingsRegistry),
		[]protocol.Protocol{ga4Protocol},
		9999,
	)
	r := newInmemReceiver(t, server)

	req := fasthttp.AcquireRequest()
	t.Cleanup(func() { fasthttp.ReleaseRequest(req) })
	req.SetRequestURI("http://localhost/g/gd.min.js")
	req.Header.SetMethod(fasthttp.MethodOptions)

	// when
	resp := r.do(req)

	// then
	assert.Equal(t, fasthttp.StatusNoContent, resp.StatusCode())
	assert.Equal(t, "*", string(resp.Header.Peek("Access-Control-Allow-Origin")))
	assert.Empty(t, string(resp.Header.Peek("Access-Control-Allow-Credentials")))
}

func TestCORS_D8A_Collect_Preflight_MirrorsOriginAndCredentials(t *testing.T) {
	// given
	storage := &captureStorageCORS{}
	settingsRegistry := buildSettingsRegistry("d8a", "test_property_id", "Test")
	d8aProtocol := d8a.NewD8AProtocol(
		currency.NewDummyConverter(1),
		properties.NewTestSettingRegistry(),
		ga4.WithPropertyIDExtractor(&staticPropertyIDExtractorCORS{propertyID: "test_property_id"}),
	)
	server := NewServer(
		storage,
		NewDummyRawLogStorage(),
		HitValidatingRuleSet(1024*128, settingsRegistry),
		[]protocol.Protocol{d8aProtocol},
		9999,
	)
	r := newInmemReceiver(t, server)

	req := fasthttp.AcquireRequest()
	t.Cleanup(func() { fasthttp.ReleaseRequest(req) })
	req.SetRequestURI("http://localhost/d/c")
	req.Header.SetMethod(fasthttp.MethodOptions)
	req.Header.Set("Origin", "https://example.test")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "content-type")

	// when
	resp := r.do(req)

	// then
	assert.Equal(t, fasthttp.StatusNoContent, resp.StatusCode())
	assert.Equal(t, "https://example.test", string(resp.Header.Peek("Access-Control-Allow-Origin")))
	assert.Equal(t, "true", string(resp.Header.Peek("Access-Control-Allow-Credentials")))
	assert.Contains(t, string(resp.Header.Peek("Vary")), "Origin")
}

func TestCORS_D8A_Static_Preflight_UsesWildcard(t *testing.T) {
	// given
	storage := &captureStorageCORS{}
	settingsRegistry := buildSettingsRegistry("d8a", "test_property_id", "Test")
	d8aProtocol := d8a.NewD8AProtocol(
		currency.NewDummyConverter(1),
		properties.NewTestSettingRegistry(),
		ga4.WithPropertyIDExtractor(&staticPropertyIDExtractorCORS{propertyID: "test_property_id"}),
	)
	server := NewServer(
		storage,
		NewDummyRawLogStorage(),
		HitValidatingRuleSet(1024*128, settingsRegistry),
		[]protocol.Protocol{d8aProtocol},
		9999,
	)
	r := newInmemReceiver(t, server)

	req := fasthttp.AcquireRequest()
	t.Cleanup(func() { fasthttp.ReleaseRequest(req) })
	req.SetRequestURI("http://localhost/d/wt.min.js")
	req.Header.SetMethod(fasthttp.MethodOptions)

	// when
	resp := r.do(req)

	// then
	assert.Equal(t, fasthttp.StatusNoContent, resp.StatusCode())
	assert.Equal(t, "*", string(resp.Header.Peek("Access-Control-Allow-Origin")))
	assert.Empty(t, string(resp.Header.Peek("Access-Control-Allow-Credentials")))
}
