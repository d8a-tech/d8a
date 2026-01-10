package d8a

import (
	_ "embed"

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
	childColumns := p.child.Columns()
	for i, column := range childColumns.Event {
		if column.Implements().ID == columns.CoreInterfaces.EventTrackingProtocol.ID {
			childColumns.Event[i] = columns.ProtocolColumn(func(_ *schema.Event) (any, schema.D8AColumnWriteError) {
				return "d8a", nil
			})
		}
	}
	return childColumns
}

func (p *d8aProtocol) Interfaces() any {
	return p.child.Interfaces()
}

//go:embed static/tracker.js
var staticTrackerJS []byte

func (p *d8aProtocol) Endpoints() []protocol.ProtocolEndpoint {
	newEndpoints := make([]protocol.ProtocolEndpoint, len(p.child.Endpoints())+1)
	for i, endpoint := range p.child.Endpoints() {
		if endpoint.Path == "/g/collect" {
			endpoint.Path = "/d/c"
		}
		newEndpoints[i] = endpoint
	}
	return append(newEndpoints, protocol.ProtocolEndpoint{
		Methods:  []string{fasthttp.MethodGet},
		Path:     "/d/js",
		IsCustom: true,
		CustomHandler: func(ctx *fasthttp.RequestCtx) {
			ctx.SetStatusCode(fasthttp.StatusOK)
			ctx.Response.Header.Set("Content-Type", "text/javascript")
			ctx.SetBody(staticTrackerJS)
		},
	})
}

func (p *d8aProtocol) Hits(ctx *fasthttp.RequestCtx, request *hits.ParsedRequest) ([]*hits.Hit, error) {
	return p.child.Hits(ctx, request)
}

func NewD8AProtocol(
	converter currency.Converter,
	psr properties.SettingsRegistry,
) protocol.Protocol {
	return &d8aProtocol{
		child: ga4.NewGA4Protocol(
			converter,
			psr,
		),
	}
}
