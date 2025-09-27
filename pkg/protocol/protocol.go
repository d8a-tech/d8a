// Package protocol defines the interface for different tracking protocol implementations.
package protocol

import (
	"io"
	"net/http"
	"net/url"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// PathProtocolMapping maps URL paths to their corresponding Protocol implementations.
type PathProtocolMapping map[string]Protocol

// Request contains all information about an incoming tracking request.
type Request struct {
	Method      []byte
	Host        []byte
	Path        []byte
	QueryParams url.Values
	Headers     http.Header
	Body        io.Reader
}

// Protocol defines the interface for different tracking protocol implementations.
type Protocol interface {
	ID() string
	Columns() schema.Columns

	Hits(*Request) ([]*hits.Hit, error)
}
