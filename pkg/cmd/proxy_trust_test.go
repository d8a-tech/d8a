package cmd

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/stretchr/testify/assert"
)

func TestTrustedProxiesOption(t *testing.T) {
	tests := []struct {
		name         string
		cidrs        []string
		expectPanics bool
	}{
		{
			name:  "nil slice does not panic",
			cidrs: nil,
		},
		{
			name:  "empty slice does not panic",
			cidrs: []string{},
		},
		{
			name:  "both universal CIDRs does not panic",
			cidrs: []string{"0.0.0.0/0", "::/0"},
		},
		{
			name:  "both universal CIDRs with extra entries does not panic",
			cidrs: []string{"10.0.0.0/8", "0.0.0.0/0", "::/0"},
		},
		{
			name:  "only IPv4 universal CIDR does not panic",
			cidrs: []string{"0.0.0.0/0"},
		},
		{
			name:  "only IPv6 universal CIDR does not panic",
			cidrs: []string{"::/0"},
		},
		{
			name:  "specific CIDRs does not panic",
			cidrs: []string{"10.0.0.0/8", "172.16.0.0/12"},
		},
		{
			name:         "invalid CIDR panics",
			cidrs:        []string{"garbage"},
			expectPanics: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanics {
				assert.Panics(t, func() {
					opt := trustedProxiesOption(tt.cidrs)
					opt(receiver.NewServer(nil, nil, nil, nil, 0))
				})
				return
			}

			// given / when — must not panic
			opt := trustedProxiesOption(tt.cidrs)
			assert.NotPanics(t, func() {
				opt(receiver.NewServer(nil, nil, nil, nil, 0))
			})
		})
	}
}

func TestContainsBothUniversalCIDRs(t *testing.T) {
	tests := []struct {
		name   string
		cidrs  []string
		expect bool
	}{
		{
			name:   "both present",
			cidrs:  []string{"0.0.0.0/0", "::/0"},
			expect: true,
		},
		{
			name:   "both present among others",
			cidrs:  []string{"10.0.0.0/8", "0.0.0.0/0", "fd00::/16", "::/0"},
			expect: true,
		},
		{
			name:   "only IPv4",
			cidrs:  []string{"0.0.0.0/0"},
			expect: false,
		},
		{
			name:   "only IPv6",
			cidrs:  []string{"::/0"},
			expect: false,
		},
		{
			name:   "neither present",
			cidrs:  []string{"10.0.0.0/8"},
			expect: false,
		},
		{
			name:   "empty",
			cidrs:  nil,
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			result := containsBothUniversalCIDRs(tt.cidrs)

			// then
			assert.Equal(t, tt.expect, result)
		})
	}
}
