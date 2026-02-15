package receiver

import (
	"context"
	"net"
	"testing"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	fasthttputil "github.com/valyala/fasthttp/fasthttputil"
)

type inmemReceiver struct {
	t      *testing.T
	client *fasthttp.Client
}

func newInmemReceiver(t *testing.T, s *Server) *inmemReceiver {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	ln := fasthttputil.NewInmemoryListener()

	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
	})

	srv := &fasthttp.Server{Handler: s.setupRouter(ctx).Handler}
	go func() {
		_ = srv.Serve(ln)
	}()

	client := &fasthttp.Client{
		Dial: func(_ string) (net.Conn, error) {
			return ln.Dial()
		},
	}

	return &inmemReceiver{t: t, client: client}
}

func (r *inmemReceiver) do(req *fasthttp.Request) *fasthttp.Response {
	r.t.Helper()

	resp := fasthttp.AcquireResponse()
	r.t.Cleanup(func() { fasthttp.ReleaseResponse(resp) })

	err := r.client.Do(req, resp)
	require.NoError(r.t, err)
	return resp
}

func buildSettingsRegistry(protocolID, propertyID, propertyName string) properties.SettingsRegistry {
	return properties.NewStaticSettingsRegistry([]properties.Settings{{
		ProtocolID:   protocolID,
		PropertyID:   propertyID,
		PropertyName: propertyName,
	}})
}
