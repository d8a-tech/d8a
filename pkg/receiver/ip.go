package receiver

import (
	"bytes"

	"github.com/valyala/fasthttp"
)

// realIP returns the client's real IP address by checking various headers
// that might contain it. The priority order is:
// 1. X-Real-IP
// 2. X-Client-IP
// 3. CF-Connecting-IP (Cloudflare)
// 4. True-Client-IP
// 5. X-Forwarded-For (leftmost non-empty value, as it can contain multiple IPs)
// 6. The actual remote IP from the connection
func realIP(ctx *fasthttp.RequestCtx) string {
	// Check each header in priority order
	headers := [][]byte{
		[]byte("X-Real-IP"),
		[]byte("X-Client-IP"),
		[]byte("CF-Connecting-IP"),
		[]byte("True-Client-IP"),
	}

	for _, header := range headers {
		if ip := ctx.Request.Header.PeekBytes(header); len(ip) > 0 {
			return string(ip)
		}
	}

	// Check X-Forwarded-For
	if xff := ctx.Request.Header.Peek("X-Forwarded-For"); len(xff) > 0 {
		// Split by comma and find first non-empty IP
		parts := bytes.Split(xff, []byte(","))
		for _, part := range parts {
			// Trim spaces and check if it's not empty
			if ip := bytes.TrimSpace(part); len(ip) > 0 {
				return string(ip)
			}
		}
	}

	// If no headers were found, use the actual remote IP
	return ctx.RemoteIP().String()
}
