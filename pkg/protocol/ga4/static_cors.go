package ga4

import (
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/valyala/fasthttp"
)

// StaticCORSEndpoint returns a custom endpoint for serving static files with
// wildcard CORS and OPTIONS preflight support.
func StaticCORSEndpoint(path, contentType string, body []byte) protocol.ProtocolEndpoint {
	return protocol.ProtocolEndpoint{
		Methods:  []string{fasthttp.MethodGet, fasthttp.MethodOptions},
		Path:     path,
		IsCustom: true,
		CustomHandler: func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Method()) == fasthttp.MethodOptions {
				ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
				ctx.Response.Header.Set("Access-Control-Allow-Methods", "GET, OPTIONS")
				ctx.Response.Header.Set("Access-Control-Allow-Headers", "Content-Type, X-Requested-With")
				ctx.Response.Header.Set("Access-Control-Max-Age", "86400")
				ctx.SetStatusCode(fasthttp.StatusNoContent)
				return
			}

			ctx.SetStatusCode(fasthttp.StatusOK)
			ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
			ctx.Response.Header.Set("Content-Type", contentType)
			ctx.SetBody(body)
		},
	}
}
