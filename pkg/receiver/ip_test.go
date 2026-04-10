package receiver

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// helper: server that trusts all proxies (old behaviour).
func trustAllServer() *Server {
	return &Server{proxyTrust: allProxyTrust{}}
}

// helper: server that trusts no proxies (new default).
func trustNoneServer() *Server {
	return &Server{proxyTrust: noProxyTrust{}}
}

// helper: server that trusts a specific CIDR list.
func trustedCIDRServer(t *testing.T, cidrs ...string) *Server {
	t.Helper()
	trust, err := newCIDRProxyTrust(cidrs)
	require.NoError(t, err)
	return &Server{proxyTrust: trust}
}

func TestRealIP_TrustAll(t *testing.T) {
	// When all proxies are trusted the behaviour matches the original realIP:
	// headers are honoured in priority order.
	tests := []struct {
		name         string
		setupRequest func() *fasthttp.RequestCtx
		expectedIP   string
	}{
		{
			name: "X-Real-IP header present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Real-IP", "1.2.3.4")
				return ctx
			},
			expectedIP: "1.2.3.4",
		},
		{
			name: "X-Client-IP header present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Client-IP", "2.3.4.5")
				return ctx
			},
			expectedIP: "2.3.4.5",
		},
		{
			name: "CF-Connecting-IP header present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("CF-Connecting-IP", "3.4.5.6")
				return ctx
			},
			expectedIP: "3.4.5.6",
		},
		{
			name: "True-Client-IP header present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("True-Client-IP", "4.5.6.7")
				return ctx
			},
			expectedIP: "4.5.6.7",
		},
		{
			name: "X-Forwarded-For header with single IP",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", "5.6.7.8")
				return ctx
			},
			expectedIP: "5.6.7.8",
		},
		{
			name: "X-Forwarded-For header with multiple IPs",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", "6.7.8.9, 7.8.9.10, 8.9.10.11")
				return ctx
			},
			expectedIP: "6.7.8.9",
		},
		{
			name: "X-Forwarded-For header with whitespace",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", "  9.10.11.12  ,10.11.12.13")
				return ctx
			},
			expectedIP: "9.10.11.12",
		},
		{
			name: "X-Forwarded-For header with empty values",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", ",,  ,10.11.12.13")
				return ctx
			},
			expectedIP: "10.11.12.13",
		},
		{
			name: "Empty X-Forwarded-For header falls back to remote IP",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Forwarded-For", "")
				ctx.SetRemoteAddr(&net.TCPAddr{
					IP:   net.ParseIP("11.12.13.14"),
					Port: 12345,
				})
				return ctx
			},
			expectedIP: "11.12.13.14",
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
			expectedIP: "1.1.1.1",
		},
		{
			name: "No headers present",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{
					IP:   net.ParseIP("12.13.14.15"),
					Port: 12345,
				})
				return ctx
			},
			expectedIP: "12.13.14.15",
		},
		{
			name: "IPv6 address in header",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.Set("X-Real-IP", "2001:db8::1")
				return ctx
			},
			expectedIP: "2001:db8::1",
		},
		{
			name: "IPv6 address via RemoteIP fallback",
			setupRequest: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{
					IP:   net.ParseIP("2001:4860:4860::8888"),
					Port: 12345,
				})
				return ctx
			},
			expectedIP: "2001:4860:4860::8888",
		},
	}

	s := trustAllServer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			ctx := tt.setupRequest()

			// when
			ip := s.realIP(ctx)

			// then
			assert.Equal(t, tt.expectedIP, ip)
		})
	}
}

func TestRealIP_TrustAllPriority(t *testing.T) {
	// given
	headers := []string{
		"X-Real-IP",
		"X-Client-IP",
		"CF-Connecting-IP",
		"True-Client-IP",
		"X-Forwarded-For",
	}
	expectedPriority := []string{
		"1.1.1.1",
		"2.2.2.2",
		"3.3.3.3",
		"4.4.4.4",
		"5.5.5.5",
	}

	s := trustAllServer()
	for i := 0; i < len(headers); i++ {
		t.Run("Priority_"+headers[i], func(t *testing.T) {
			// given
			ctx := &fasthttp.RequestCtx{}
			for j := i; j < len(headers); j++ {
				ctx.Request.Header.Set(headers[j], expectedPriority[j])
			}

			// when
			ip := s.realIP(ctx)

			// then
			assert.Equal(t, expectedPriority[i], ip)
		})
	}
}

func TestRealIP_TrustNone_IgnoresHeaders(t *testing.T) {
	// Default (no trusted proxies): headers are always ignored;
	// the TCP remote address is returned regardless of headers.
	tests := []struct {
		name       string
		setup      func() *fasthttp.RequestCtx
		expectedIP string
	}{
		{
			name: "X-Real-IP ignored",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 1})
				ctx.Request.Header.Set("X-Real-IP", "1.2.3.4")
				return ctx
			},
			expectedIP: "10.0.0.1",
		},
		{
			name: "X-Forwarded-For ignored",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("10.0.0.2"), Port: 1})
				ctx.Request.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
				return ctx
			},
			expectedIP: "10.0.0.2",
		},
		{
			name: "CF-Connecting-IP ignored",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("10.0.0.3"), Port: 1})
				ctx.Request.Header.Set("CF-Connecting-IP", "9.9.9.9")
				return ctx
			},
			expectedIP: "10.0.0.3",
		},
		{
			name: "all headers ignored",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("10.0.0.4"), Port: 1})
				ctx.Request.Header.Set("X-Real-IP", "1.1.1.1")
				ctx.Request.Header.Set("X-Client-IP", "2.2.2.2")
				ctx.Request.Header.Set("CF-Connecting-IP", "3.3.3.3")
				ctx.Request.Header.Set("True-Client-IP", "4.4.4.4")
				ctx.Request.Header.Set("X-Forwarded-For", "5.5.5.5")
				return ctx
			},
			expectedIP: "10.0.0.4",
		},
		{
			name: "no headers returns remote IP",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1})
				return ctx
			},
			expectedIP: "192.168.1.1",
		},
	}

	s := trustNoneServer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			ctx := tt.setup()

			// when
			ip := s.realIP(ctx)

			// then
			assert.Equal(t, tt.expectedIP, ip)
		})
	}
}

func TestRealIP_TrustedCIDR(t *testing.T) {
	// Only trust proxies in 10.0.0.0/24 and the single IP 172.16.0.1.
	s := trustedCIDRServer(t, "10.0.0.0/24", "172.16.0.1")

	tests := []struct {
		name       string
		setup      func() *fasthttp.RequestCtx
		expectedIP string
	}{
		{
			name: "trusted proxy - header honoured",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 1})
				ctx.Request.Header.Set("X-Real-IP", "203.0.113.50")
				return ctx
			},
			expectedIP: "203.0.113.50",
		},
		{
			name: "trusted single IP - header honoured",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("172.16.0.1"), Port: 1})
				ctx.Request.Header.Set("X-Forwarded-For", "198.51.100.10, 10.0.0.1")
				return ctx
			},
			expectedIP: "198.51.100.10",
		},
		{
			name: "untrusted proxy - header ignored",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1})
				ctx.Request.Header.Set("X-Real-IP", "203.0.113.50")
				return ctx
			},
			expectedIP: "192.168.1.1",
		},
		{
			name: "untrusted proxy outside CIDR - header ignored",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("10.0.1.1"), Port: 1})
				ctx.Request.Header.Set("X-Real-IP", "203.0.113.50")
				return ctx
			},
			expectedIP: "10.0.1.1",
		},
		{
			name: "trusted proxy - no header falls back to remote",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("10.0.0.100"), Port: 1})
				return ctx
			},
			expectedIP: "10.0.0.100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			ctx := tt.setup()

			// when
			ip := s.realIP(ctx)

			// then
			assert.Equal(t, tt.expectedIP, ip)
		})
	}
}

func TestRealIP_TrustedCIDR_IPv6(t *testing.T) {
	s := trustedCIDRServer(t, "fd00::/16")

	tests := []struct {
		name       string
		setup      func() *fasthttp.RequestCtx
		expectedIP string
	}{
		{
			name: "trusted IPv6 proxy - header honoured",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("fd00::1"), Port: 1})
				ctx.Request.Header.Set("X-Real-IP", "2001:db8::42")
				return ctx
			},
			expectedIP: "2001:db8::42",
		},
		{
			name: "untrusted IPv6 proxy - header ignored",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("2001:db8::99"), Port: 1})
				ctx.Request.Header.Set("X-Real-IP", "2001:db8::42")
				return ctx
			},
			expectedIP: "2001:db8::99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			ctx := tt.setup()

			// when
			ip := s.realIP(ctx)

			// then
			assert.Equal(t, tt.expectedIP, ip)
		})
	}
}

func TestNewCIDRProxyTrust_InvalidCIDR(t *testing.T) {
	_, err := newCIDRProxyTrust([]string{"not-an-ip"})
	assert.Error(t, err)
}

func TestNewCIDRProxyTrust_InvalidCIDRNotation(t *testing.T) {
	_, err := newCIDRProxyTrust([]string{"10.0.0.0/abc"})
	assert.Error(t, err)
}

func TestWithTrustedProxies_PanicsOnInvalidCIDR(t *testing.T) {
	assert.Panics(t, func() {
		WithTrustedProxies("garbage")(&Server{})
	})
}

// customProxyTrust is a hand-written stub that trusts only one specific IP.
type customProxyTrust struct {
	trustedIP net.IP
}

func (c customProxyTrust) IsTrustedProxy(ip net.IP) bool {
	return c.trustedIP.Equal(ip)
}

func TestWithProxyTrust_CustomImplementation(t *testing.T) {
	// given — trust only 10.1.2.3
	trust := customProxyTrust{trustedIP: net.ParseIP("10.1.2.3")}
	s := NewServer(nil, nil, nil, nil, 0, WithProxyTrust(trust))

	tests := []struct {
		name       string
		setup      func() *fasthttp.RequestCtx
		expectedIP string
	}{
		{
			name: "custom trusted proxy - header honoured",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 1})
				ctx.Request.Header.Set("X-Real-IP", "203.0.113.7")
				return ctx
			},
			expectedIP: "203.0.113.7",
		},
		{
			name: "untrusted proxy - header ignored",
			setup: func() *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("10.1.2.4"), Port: 1})
				ctx.Request.Header.Set("X-Real-IP", "203.0.113.7")
				return ctx
			},
			expectedIP: "10.1.2.4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			ctx := tt.setup()

			// when
			ip := s.realIP(ctx)

			// then
			assert.Equal(t, tt.expectedIP, ip)
		})
	}
}

func TestNewServer_WithProxyTrust_StoresCustomImplementation(t *testing.T) {
	// given
	trust := customProxyTrust{trustedIP: net.ParseIP("1.2.3.4")}

	// when
	s := NewServer(nil, nil, nil, nil, 0, WithProxyTrust(trust))

	// then
	assert.Equal(t, trust, s.proxyTrust)
}

func TestNewServer_DefaultProxyTrust(t *testing.T) {
	// given / when
	s := NewServer(nil, nil, nil, nil, 0)

	// then — default must be no trust
	assert.IsType(t, noProxyTrust{}, s.proxyTrust)
}

func TestNewServer_WithTrustAllProxies(t *testing.T) {
	// given / when
	s := NewServer(nil, nil, nil, nil, 0, WithTrustAllProxies())

	// then
	assert.IsType(t, allProxyTrust{}, s.proxyTrust)
}
