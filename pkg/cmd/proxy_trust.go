package cmd

import "github.com/d8a-tech/d8a/pkg/receiver"

// universalIPv4CIDR and universalIPv6CIDR are the CIDRs that, when both
// present, indicate that all proxies should be trusted.
const (
	universalIPv4CIDR = "0.0.0.0/0"
	universalIPv6CIDR = "::/0"
)

// trustedProxiesOption maps a --server-trusted-proxies string slice to the
// appropriate receiver.ServerOption:
//   - empty slice → no proxies trusted (default)
//   - both 0.0.0.0/0 and ::/0 present → trust all proxies
//   - otherwise → CIDR-based trust
func trustedProxiesOption(cidrs []string) receiver.ServerOption {
	if len(cidrs) == 0 {
		return noopServerOption
	}

	if containsBothUniversalCIDRs(cidrs) {
		return receiver.WithTrustAllProxies()
	}

	return receiver.WithTrustedProxies(cidrs...)
}

func containsBothUniversalCIDRs(cidrs []string) bool {
	var hasV4, hasV6 bool
	for _, c := range cidrs {
		switch c {
		case universalIPv4CIDR:
			hasV4 = true
		case universalIPv6CIDR:
			hasV6 = true
		}
	}
	return hasV4 && hasV6
}

func noopServerOption(*receiver.Server) {}
