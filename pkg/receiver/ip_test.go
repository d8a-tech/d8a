package receiver

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

func TestRealIP(t *testing.T) {
	tests := []struct {
		name         string
		setupRequest func() *fasthttp.RequestCtx
		expectedIP   string
		description  string
	}{
		{
			name: "X-Real-IP header present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Real-IP", "1.2.3.4")
				return ctx
			},
			expectedIP:  "1.2.3.4",
			description: "Should use X-Real-IP header value when present",
		},
		{
			name: "X-Client-IP header present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Client-IP", "2.3.4.5")
				return ctx
			},
			expectedIP:  "2.3.4.5",
			description: "Should use X-Client-IP header value when present and X-Real-IP is absent",
		},
		{
			name: "CF-Connecting-IP header present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("CF-Connecting-IP", "3.4.5.6")
				return ctx
			},
			expectedIP:  "3.4.5.6",
			description: "Should use CF-Connecting-IP header value when other high priority headers are absent",
		},
		{
			name: "True-Client-IP header present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("True-Client-IP", "4.5.6.7")
				return ctx
			},
			expectedIP:  "4.5.6.7",
			description: "Should use True-Client-IP header value when other high priority headers are absent",
		},
		{
			name: "X-Forwarded-For header with single IP",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", "5.6.7.8")
				return ctx
			},
			expectedIP: "5.6.7.8",
			description: "Should use X-Forwarded-For header value when it contains a single IP and other " +
				"high priority headers are absent",
		},
		{
			name: "X-Forwarded-For header with multiple IPs",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", "6.7.8.9, 7.8.9.10, 8.9.10.11")
				return ctx
			},
			expectedIP:  "6.7.8.9",
			description: "Should use the leftmost IP in X-Forwarded-For header when it contains multiple IPs",
		},
		{
			name: "X-Forwarded-For header with whitespace",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", "  9.10.11.12  ,10.11.12.13")
				return ctx
			},
			expectedIP:  "9.10.11.12",
			description: "Should trim whitespace from IPs in X-Forwarded-For header",
		},
		{
			name: "X-Forwarded-For header with empty values",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", ",,  ,10.11.12.13")
				return ctx
			},
			expectedIP:  "10.11.12.13",
			description: "Should skip empty values in X-Forwarded-For header",
		},
		{
			name: "Empty X-Forwarded-For header",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", "")
				// Set remote IP for this test case
				ctx.SetRemoteAddr(&net.TCPAddr{
					IP:   net.ParseIP("11.12.13.14"),
					Port: 12345,
				})
				return ctx
			},
			expectedIP:  "11.12.13.14",
			description: "Should fall back to remote IP when X-Forwarded-For is empty",
		},
		{
			name: "Multiple headers present - precedence order",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Real-IP", "1.1.1.1")
				ctx.Request.Header.Set("X-Client-IP", "2.2.2.2")
				ctx.Request.Header.Set("CF-Connecting-IP", "3.3.3.3")
				ctx.Request.Header.Set("True-Client-IP", "4.4.4.4")
				ctx.Request.Header.Set("X-Forwarded-For", "5.5.5.5")
				return ctx
			},
			expectedIP:  "1.1.1.1",
			description: "Should respect header precedence order when multiple headers are present",
		},
		{
			name: "No headers present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				// Set remote IP for this test case
				ctx.SetRemoteAddr(&net.TCPAddr{
					IP:   net.ParseIP("12.13.14.15"),
					Port: 12345,
				})
				return ctx
			},
			expectedIP:  "12.13.14.15",
			description: "Should use the remote IP when no headers are present",
		},
		{
			name: "IPv6 address in header",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Real-IP", "2001:db8::1")
				return ctx
			},
			expectedIP:  "2001:db8::1",
			description: "Should handle IPv6 addresses correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			ctx := tt.setupRequest()

			// when
			ip := realIP(ctx)

			// then
			assert.Equal(t, tt.expectedIP, ip, tt.description)
		})
	}
}

// TestRealIPPriority tests that the realIP function respects the priority order
// of headers when multiple headers are present.
func TestRealIPPriority(t *testing.T) {
	// given
	headers := []string{
		"X-Real-IP",
		"X-Client-IP",
		"CF-Connecting-IP",
		"True-Client-IP",
		"X-Forwarded-For",
	}
	expectedPriority := []string{
		"1.1.1.1", // X-Real-IP
		"2.2.2.2", // X-Client-IP
		"3.3.3.3", // CF-Connecting-IP
		"4.4.4.4", // True-Client-IP
		"5.5.5.5", // X-Forwarded-For
	}

	// Test removing each header one by one from highest to lowest priority
	for i := 0; i < len(headers); i++ {
		t.Run("Priority_"+headers[i], func(t *testing.T) {
			// given
			ctx := &fasthttp.RequestCtx{}

			// Set all headers below the current priority level
			for j := i; j < len(headers); j++ {
				ctx.Request.Header.Set(headers[j], expectedPriority[j])
			}

			// when
			ip := realIP(ctx)

			// then
			assert.Equal(t, expectedPriority[i], ip,
				"Should use "+headers[i]+" when higher priority headers are absent")
		})
	}
}
