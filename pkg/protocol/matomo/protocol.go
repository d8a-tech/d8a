package matomo

import (
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/valyala/fasthttp"
)

type matomoProtocol struct{}

func (p *matomoProtocol) ID() string {
	return "matomo"
}

func (p *matomoProtocol) Hits(_ *fasthttp.RequestCtx, _ *hits.ParsedRequest) ([]*hits.Hit, error) {
	return nil, nil
}

func (p *matomoProtocol) Endpoints() []protocol.ProtocolEndpoint {
	return []protocol.ProtocolEndpoint{
		{
			Methods: []string{fasthttp.MethodPost},
			Path:    "/matomo.php",
		},
	}
}

func (p *matomoProtocol) Interfaces() any {
	return nil
}

func (p *matomoProtocol) Columns() schema.Columns {
	return schema.Columns{
		Event:              eventColumns,
		Session:            sessionColumns,
		SessionScopedEvent: sseColumns,
	}
}

func NewMatomoProtocol() protocol.Protocol {
	return &matomoProtocol{}
}
