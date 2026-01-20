package d8a

import (
	_ "embed"
	"strings"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/valyala/fasthttp"
)

type d8aProtocol struct {
	child protocol.Protocol
}

func (p *d8aProtocol) ID() string {
	return "d8a"
}

func (p *d8aProtocol) Columns() schema.Columns {
	return WrapColumns(
		p.child.Columns(),
		WithPatchEvent(
			columns.CoreInterfaces.EventTrackingProtocol.ID,
			columns.ProtocolColumn(func(_ *schema.Event) (any, schema.D8AColumnWriteError) {
				return "d8a", nil
			}),
		),
	)
}

func (p *d8aProtocol) Interfaces() any {
	return p.child.Interfaces()
}

//go:embed static/wt.min.js
var staticWebTracker []byte

//go:embed static/wt.min.js.map
var staticWebTrackerMap []byte

func (p *d8aProtocol) Endpoints() []protocol.ProtocolEndpoint {
	newEndpoints := make([]protocol.ProtocolEndpoint, 0)
	for _, endpoint := range p.child.Endpoints() {
		if endpoint.Path == "/g/collect" {
			// Decorate only the tracking endpoint
			endpoint.Path = "/d/c"
		} else if strings.HasPrefix(endpoint.Path, "/g/") {
			// Ignore all the others (static files, etc.)
			continue
		}
		newEndpoints = append(newEndpoints, endpoint)
	}
	return append(newEndpoints, []protocol.ProtocolEndpoint{
		{
			Methods:  []string{fasthttp.MethodGet},
			Path:     "/d/wt.min.js",
			IsCustom: true,
			CustomHandler: func(ctx *fasthttp.RequestCtx) {
				ctx.SetStatusCode(fasthttp.StatusOK)
				ctx.Response.Header.Set("Content-Type", "text/javascript")
				ctx.SetBody(staticWebTracker)
			},
		},
		{
			Methods:  []string{fasthttp.MethodGet},
			Path:     "/d/wt.min.js.map",
			IsCustom: true,
			CustomHandler: func(ctx *fasthttp.RequestCtx) {
				ctx.SetStatusCode(fasthttp.StatusOK)
				ctx.Response.Header.Set("Content-Type", "application/json")
				ctx.SetBody(staticWebTrackerMap)
			},
		},
	}...)
}

func (p *d8aProtocol) Hits(ctx *fasthttp.RequestCtx, request *hits.ParsedRequest) ([]*hits.Hit, error) {
	return p.child.Hits(ctx, request)
}

func NewD8AProtocol(
	converter currency.Converter,
	psr properties.SettingsRegistry,
	opts ...ga4.GA4ProtocolOption,
) protocol.Protocol {
	return &d8aProtocol{
		child: ga4.NewGA4Protocol(
			converter,
			psr,
			opts...,
		),
	}
}
