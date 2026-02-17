package filter

import (
	"net"
	"regexp"
	"strings"

	expr "github.com/expr-lang/expr"
)

// startsWith checks if a string starts with a prefix.
func startsWith(params ...any) (any, error) {
	if len(params) < 2 {
		return false, nil
	}
	s, ok := params[0].(string)
	if !ok {
		return false, nil
	}
	prefix, ok := params[1].(string)
	if !ok {
		return false, nil
	}
	return strings.HasPrefix(s, prefix), nil
}

// endsWith checks if a string ends with a suffix.
func endsWith(params ...any) (any, error) {
	if len(params) < 2 {
		return false, nil
	}
	s, ok := params[0].(string)
	if !ok {
		return false, nil
	}
	suffix, ok := params[1].(string)
	if !ok {
		return false, nil
	}
	return strings.HasSuffix(s, suffix), nil
}

// contains checks if a string contains a substring.
func contains(params ...any) (any, error) {
	if len(params) < 2 {
		return false, nil
	}
	s, ok := params[0].(string)
	if !ok {
		return false, nil
	}
	substr, ok := params[1].(string)
	if !ok {
		return false, nil
	}
	return strings.Contains(s, substr), nil
}

// inCIDR checks if an IP address is within a CIDR range.
func inCIDR(params ...any) (any, error) {
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

// matches checks if a string matches a regular expression pattern.
func matches(params ...any) (any, error) {
	if len(params) < 2 {
		return false, nil
	}
	s, ok := params[0].(string)
	if !ok {
		return false, nil
	}
	pattern, ok := params[1].(string)
	if !ok {
		return false, nil
	}

	matched, err := regexp.MatchString(pattern, s)
	if err != nil {
		return false, nil
	}
	return matched, nil
}

// FunctionOptions returns expr.Option values to register custom functions.
func FunctionOptions() []expr.Option {
	return []expr.Option{
		expr.Function("starts_with", startsWith, new(func(string, string) bool)),
		expr.Function("ends_with", endsWith, new(func(string, string) bool)),
		expr.Function("contains", contains, new(func(string, string) bool)),
		expr.Function("in_cidr", inCIDR, new(func(string, string) bool)),
		expr.Function("matches", matches, new(func(string, string) bool)),
	}
}
