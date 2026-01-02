package d8a

import (
	_ "embed"

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
	return p.child.Columns()
}

func (p *d8aProtocol) Interfaces() any {
	return p.child.Interfaces()
}

//go:embed static/tracker.js
var staticTrackerJS []byte

func (p *d8aProtocol) Endpoints() []protocol.ProtocolEndpoint {
	return append(p.child.Endpoints(), protocol.ProtocolEndpoint{
		Methods:  []string{fasthttp.MethodGet},
		Path:     "/",
		IsCustom: true,
		CustomHandler: func(ctx *fasthttp.RequestCtx) {
			ctx.SetStatusCode(fasthttp.StatusOK)
			ctx.Response.Header.Set("Content-Type", "text/javascript")
			ctx.SetBody(staticTrackerJS)
		},
	})
}

func (p *d8aProtocol) Hits(request *hits.Request) ([]*hits.Hit, error) {
	return p.child.Hits(request)
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
