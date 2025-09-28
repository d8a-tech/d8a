package receiver

import (
	"fmt"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
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
}

func (m *mockProtocol) ID() string {
	return m.id
}

func (m *mockProtocol) Hits(request *protocol.Request) ([]*hits.Hit, error) {
	theHit := hits.New()

	theHit.ClientID = hits.ClientID("test_client_id")
	theHit.AuthoritativeClientID = theHit.ClientID
	theHit.PropertyID = "test_property_id"
	theHit.Host = string(request.Host)
	theHit.Path = string(request.Path)
	theHit.Method = string(request.Method)
	theHit.Headers = url.Values{}
	for key, values := range request.Headers {
		for _, value := range values {
			theHit.Headers.Add(key, value)
		}
	}
	theHit.QueryParams = request.QueryParams
	var err error
	var body []byte
	if request.Body != nil {
		body, err = io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
	}
	theHit.Body = body
	theHit.Timestamp = time.Now()
	return []*hits.Hit{theHit}, m.err
}

func (m *mockProtocol) Columns() schema.Columns {
	return m.columns
}

func TestHandleRequest(t *testing.T) {
	tests := []struct {
		name           string
		request        func() *fasthttp.RequestCtx
		protocolMap    protocol.PathProtocolMapping
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
			protocolMap: protocol.PathProtocolMapping{
				"/collect": &mockProtocol{id: "test_protocol"},
			},
			storageErr:     nil,
			expectedStatus: fasthttp.StatusNoContent,
			validateHit: func(t *testing.T, hit *hits.Hit) {
				assert.Equal(t, "192.168.1.1", hit.IP)
				assert.Equal(t, "example.com", hit.Host)
				assert.Equal(t, "/collect", hit.Path)
				assert.Equal(t, "GET", hit.Method)
				assert.Equal(t, []string{"value1"}, hit.QueryParams["param1"])
				assert.Equal(t, []string{"value2"}, hit.QueryParams["param2"])
				assert.Equal(t, []string{"test-agent"}, hit.Headers["User-Agent"])
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

			// when
			handleRequest(ctx, storage, NewDummyRawLogStorage(), tt.protocolMap["/collect"])

			// then
			fmt.Println(string(ctx.Response.Body()))
			assert.Equal(t, tt.expectedStatus, ctx.Response.StatusCode())
			if tt.validateHit != nil && len(storage.hits) > 0 {
				tt.validateHit(t, storage.hits[0])
			}
		})
	}
}
