// Package protocol defines the interface for different tracking protocol implementations.
package protocol

import (
	"io"
	"net/http"
	"net/url"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/valyala/fasthttp"
)

// Request contains all information about an incoming tracking request.
type Request struct {
	Method      []byte
	Host        []byte
	Path        []byte
	QueryParams url.Values
	Headers     http.Header
	Body        io.Reader
}

// ProtocolEndpoint describes a web endpoint that is used by a protocol.
type ProtocolEndpoint struct {
	Methods []string
	Path    string
	// Normal endpoints are hit-creating ones, Custom endpoints are called directly
	// and may serve different purposes.
	IsCustom      bool
	CustomHandler func(*fasthttp.RequestCtx)
}

// Protocol defines the interface for different tracking protocol implementations.
type Protocol interface {
	ID() string
	Columns() schema.Columns
	Interfaces() any
	Endpoints() []ProtocolEndpoint
	Hits(*fasthttp.RequestCtx, *hits.ParsedRequest) ([]*hits.Hit, error)
}
