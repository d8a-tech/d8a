package splitter

import (
	"net"

	expr "github.com/expr-lang/expr"
)

// inCidr checks if an IP address is within a CIDR range.
func inCidr(params ...any) (any, error) {
	if len(params) < 2 {
		return false, nil
	}
	ip, ok := params[0].(string)
	if !ok {
		return false, nil
	}
	cidr, ok := params[1].(string)
	if !ok {
		return false, nil
	}

	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return false, nil
	}
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false, nil
	}
	return ipNet.Contains(parsedIP), nil
}

// FunctionOptions returns expr.Option values to register custom functions.
func FunctionOptions() []expr.Option {
	return []expr.Option{
		expr.Function("inCidr", inCidr, new(func(string, string) bool)),
	}
}
