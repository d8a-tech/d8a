package receiver

import (
	"bytes"
	"net"

	"github.com/valyala/fasthttp"
)

// ProxyTrust controls whether and which proxy-set headers are trusted
// when extracting the client's real IP address.
//
// Implement this interface and pass it to WithProxyTrust to inject fully
// custom proxy-trust logic.
type ProxyTrust interface {
	IsTrustedProxy(ip net.IP) bool
}

// noProxyTrust trusts no proxy — headers are always ignored.
type noProxyTrust struct{}

func (noProxyTrust) IsTrustedProxy(net.IP) bool { return false }

// allProxyTrust trusts every proxy — headers are always honoured.
type allProxyTrust struct{}

func (allProxyTrust) IsTrustedProxy(net.IP) bool { return true }

// cidrProxyTrust trusts only proxies whose remote address falls within
// one of the configured CIDR ranges.
type cidrProxyTrust struct {
	nets []*net.IPNet
}

func newCIDRProxyTrust(cidrs []string) (*cidrProxyTrust, error) {
	nets := make([]*net.IPNet, 0, len(cidrs))
	for _, cidr := range cidrs {
		// Allow bare IPs by appending a full-width mask.
		if !bytes.Contains([]byte(cidr), []byte("/")) {
			ip := net.ParseIP(cidr)
			if ip == nil {
				return nil, &net.ParseError{Type: "CIDR address", Text: cidr}
			}
			if ip.To4() != nil {
				cidr += "/32"
			} else {
				cidr += "/128"
			}
		}
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, err
		}
		nets = append(nets, ipNet)
	}
	return &cidrProxyTrust{nets: nets}, nil
}

func (t *cidrProxyTrust) IsTrustedProxy(ip net.IP) bool {
	for _, n := range t.nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// WithProxyTrust configures the server to use a custom ProxyTrust
// implementation for deciding which proxies are trusted. This is the escape
// hatch for scenarios where neither WithTrustAllProxies nor
// WithTrustedProxies fits (e.g. dynamic allow-lists, cloud-metadata lookups).
func WithProxyTrust(trust ProxyTrust) ServerOption {
	return func(s *Server) {
		s.proxyTrust = trust
	}
}

// WithTrustAllProxies configures the server to trust all proxies, honouring
// forwarded-IP headers from any source. Use only behind a reverse proxy that
// sanitises these headers.
func WithTrustAllProxies() ServerOption {
	return func(s *Server) {
		s.proxyTrust = allProxyTrust{}
	}
}

// WithTrustedProxies configures the server to trust forwarded-IP headers only
// when the immediate connection comes from one of the given CIDRs or IPs.
// Bare IP addresses (without a prefix length) are treated as /32 (IPv4) or
// /128 (IPv6). Returns a fatal-level log and falls back to no-trust if any
// entry is unparseable.
func WithTrustedProxies(cidrs ...string) ServerOption {
	return func(s *Server) {
		trust, err := newCIDRProxyTrust(cidrs)
		if err != nil {
			// Fail loudly — a mis-configured allow-list is a security risk.
			panic("receiver: invalid trusted proxy CIDR: " + err.Error())
		}
		s.proxyTrust = trust
	}
}

// realIP returns the client's real IP address (IPv4 or IPv6).
//
// When the immediate connection originates from a trusted proxy, the following
// headers are inspected in priority order:
//  1. X-Real-IP
//  2. X-Client-IP
//  3. CF-Connecting-IP (Cloudflare)
//  4. True-Client-IP
//  5. X-Forwarded-For (leftmost non-empty value)
//
// When the proxy is not trusted (the default), headers are ignored and the TCP
// remote address is returned directly. This prevents IP spoofing by clients
// that are not behind a trusted reverse proxy.
func (s *Server) realIP(ctx *fasthttp.RequestCtx) string {
	remoteIP := ctx.RemoteIP()

	if !s.proxyTrust.IsTrustedProxy(remoteIP) {
		return remoteIP.String()
	}

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

	if xff := ctx.Request.Header.Peek("X-Forwarded-For"); len(xff) > 0 {
		parts := bytes.Split(xff, []byte(","))
		for _, part := range parts {
			if ip := bytes.TrimSpace(part); len(ip) > 0 {
				return string(ip)
			}
		}
	}

	return remoteIP.String()
}
