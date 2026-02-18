package splitter

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCustomFunctions(t *testing.T) {
	// given
	tests := []struct {
		name     string
		function func(...any) (any, error)
		args     []any
		want     bool
	}{
		{
			name:     "starts_with matches",
			function: startsWith,
			args:     []any{"192.168.1.1", "192.168"},
			want:     true,
		},
		{
			name:     "starts_with no match",
			function: startsWith,
			args:     []any{"10.0.0.1", "192.168"},
			want:     false,
		},
		{
			name:     "ends_with matches",
			function: endsWith,
			args:     []any{"device.example.com", ".100"},
			want:     false,
		},
		{
			name:     "ends_with matches with dot",
			function: endsWith,
			args:     []any{"192.168.1.100", ".100"},
			want:     true,
		},
		{
			name:     "contains matches",
			function: contains,
			args:     []any{"192.168.1.1", "168.1"},
			want:     true,
		},
		{
			name:     "contains no match",
			function: contains,
			args:     []any{"10.0.0.1", "168.1"},
			want:     false,
		},
		{
			name:     "matches regex matches",
			function: matches,
			args:     []any{"10.0.0.5", `^10\.0\.0\.[0-9]{1,3}$`},
			want:     true,
		},
		{
			name:     "matches regex no match",
			function: matches,
			args:     []any{"10.0.0.30", "^10\\.0\\.0\\.(1[0-9]|2[0-5])$"},
			want:     false,
		},
		{
			name:     "in_cidr matches IPv4",
			function: inCIDR,
			args:     []any{"192.168.1.50", "192.168.0.0/16"},
			want:     true,
		},
		{
			name:     "in_cidr no match IPv4",
			function: inCIDR,
			args:     []any{"10.0.0.1", "192.168.0.0/16"},
			want:     false,
		},
		{
			name:     "in_cidr matches private",
			function: inCIDR,
			args:     []any{"10.5.0.1", "10.0.0.0/8"},
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			result, err := tt.function(tt.args...)
			// then
			assert.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestFilterModifierExcludeActive(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"ip_address"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "block_internal",
				Type:       properties.FilterTypeExclude,
				Active:     true,
				Expression: `starts_with(ip_address, "192.168")`,
			},
		},
	}
	modifier, err := NewFilter(config)
	require.NoError(t, err)

	session := &schema.Session{
		Events: []*schema.Event{
			{Values: map[string]any{"ip_address": "192.168.1.1"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "8.8.8.8"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "192.168.2.1"}, Metadata: make(map[string]any)},
		},
	}

	// when
	sessions, err := modifier.Split(session)
	// then
	assert.NoError(t, err)
	assert.Len(t, sessions, 1)
	assert.Len(t, sessions[0].Events, 1)
	assert.Equal(t, "8.8.8.8", sessions[0].Events[0].Values["ip_address"])
}

func TestFilterModifierAllowActive(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"ip_address"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "vpn_only",
				Type:       properties.FilterTypeAllow,
				Active:     true,
				Expression: `in_cidr(ip_address, "100.64.0.0/10")`,
			},
		},
	}
	modifier, err := NewFilter(config)
	require.NoError(t, err)

	session := &schema.Session{
		Events: []*schema.Event{
			{Values: map[string]any{"ip_address": "100.65.0.1"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "8.8.8.8"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "100.127.255.254"}, Metadata: make(map[string]any)},
		},
	}

	// when
	sessions, err := modifier.Split(session)
	// then
	assert.NoError(t, err)
	assert.Len(t, sessions, 1)
	assert.Len(t, sessions[0].Events, 2)
	assert.Equal(t, "100.65.0.1", sessions[0].Events[0].Values["ip_address"])
	assert.Equal(t, "100.127.255.254", sessions[0].Events[1].Values["ip_address"])
}

func TestFilterModifierTestingMode(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"ip_address"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "test_office",
				Type:       properties.FilterTypeExclude,
				Active:     false,
				Expression: `ip_address == "203.0.113.50"`,
			},
		},
	}
	modifier, err := NewFilter(config)
	require.NoError(t, err)

	session := &schema.Session{
		Events: []*schema.Event{
			{Values: map[string]any{"ip_address": "203.0.113.50"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "8.8.8.8"}, Metadata: make(map[string]any)},
		},
	}

	// when
	sessions, err := modifier.Split(session)
	// then
	assert.NoError(t, err)
	assert.Len(t, sessions, 1)
	// All events should still be present in testing mode
	assert.Len(t, sessions[0].Events, 2)
	// Matching event should have traffic_type metadata
	assert.Equal(t, "test_office", sessions[0].Events[0].Metadata["traffic_filter_name"])
	// Non-matching event should not have traffic_type metadata
	_, ok := sessions[0].Events[1].Metadata["traffic_filter_name"]
	assert.False(t, ok)
}

func TestFilterModifierComplexExpression(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"ip_address"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "internal_or_vpn",
				Type:       properties.FilterTypeExclude,
				Active:     true,
				Expression: `starts_with(ip_address, "192.168") || in_cidr(ip_address, "10.0.0.0/8")`,
			},
		},
	}
	modifier, err := NewFilter(config)
	require.NoError(t, err)

	session := &schema.Session{
		Events: []*schema.Event{
			{Values: map[string]any{"ip_address": "192.168.1.1"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "10.5.0.1"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "8.8.8.8"}, Metadata: make(map[string]any)},
		},
	}

	// when
	sessions, err := modifier.Split(session)
	// then
	assert.NoError(t, err)
	assert.Len(t, sessions, 1)
	assert.Len(t, sessions[0].Events, 1)
	assert.Equal(t, "8.8.8.8", sessions[0].Events[0].Values["ip_address"])
}

func TestFilterModifierAllEventsFiltered(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"ip_address"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "block_all",
				Type:       properties.FilterTypeExclude,
				Active:     true,
				Expression: `ip_address != ""`,
			},
		},
	}
	modifier, err := NewFilter(config)
	require.NoError(t, err)

	session := &schema.Session{
		Events: []*schema.Event{
			{Values: map[string]any{"ip_address": "192.168.1.1"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "8.8.8.8"}, Metadata: make(map[string]any)},
		},
	}

	// when
	sessions, err := modifier.Split(session)
	// then
	assert.NoError(t, err)
	assert.Len(t, sessions, 0)
}

func TestFilterModifierEmptyConditions(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields:     []string{"ip_address"},
		Conditions: []properties.ConditionConfig{},
	}
	modifier, err := NewFilter(config)
	require.NoError(t, err)

	session := &schema.Session{
		Events: []*schema.Event{
			{Values: map[string]any{"ip_address": "192.168.1.1"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "8.8.8.8"}, Metadata: make(map[string]any)},
		},
	}

	// when
	sessions, err := modifier.Split(session)
	// then
	assert.NoError(t, err)
	assert.Len(t, sessions, 1)
	assert.Len(t, sessions[0].Events, 2)
}

func TestFilterModifierInvalidExpression(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"ip_address"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "invalid",
				Type:       properties.FilterTypeExclude,
				Active:     true,
				Expression: `invalid syntax here [`,
			},
		},
	}

	// when
	_, err := NewFilter(config)
	// then
	assert.Error(t, err)
}

func TestFilterModifierMissingField(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"missing_field"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "check_missing",
				Type:       properties.FilterTypeExclude,
				Active:     true,
				Expression: `missing_field == "test"`,
			},
		},
	}
	modifier, err := NewFilter(config)
	require.NoError(t, err)

	session := &schema.Session{
		Events: []*schema.Event{
			{Values: map[string]any{"ip_address": "192.168.1.1"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "8.8.8.8"}, Metadata: make(map[string]any)},
		},
	}

	// when - should not crash, handles gracefully
	sessions, err := modifier.Split(session)
	// then
	assert.NoError(t, err)
	// When field is missing, default empty string doesn't match, so events are kept
	assert.Len(t, sessions, 1)
	assert.Len(t, sessions[0].Events, 2)
}

func TestFilterModifierCIDREdgeCases(t *testing.T) {
	// given
	tests := []struct {
		name       string
		ip         string
		cidr       string
		wantInside bool
	}{
		{
			name:       "boundary IP in /10 CIDR",
			ip:         "100.64.0.0",
			cidr:       "100.64.0.0/10",
			wantInside: true,
		},
		{
			name:       "last IP in /10 CIDR",
			ip:         "100.127.255.255",
			cidr:       "100.64.0.0/10",
			wantInside: true,
		},
		{
			name:       "just outside /10 CIDR",
			ip:         "100.128.0.0",
			cidr:       "100.64.0.0/10",
			wantInside: false,
		},
		{
			name:       "IPv6 in CIDR",
			ip:         "2001:db8::1",
			cidr:       "2001:db8::/32",
			wantInside: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			result, err := inCIDR(tt.ip, tt.cidr)
			// then
			assert.NoError(t, err)
			assert.Equal(t, tt.wantInside, result)
		})
	}
}

func TestFilterModifierMultipleConditions(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"ip_address"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "exclude_internal",
				Type:       properties.FilterTypeExclude,
				Active:     true,
				Expression: `starts_with(ip_address, "192.168")`,
			},
			{
				Name:       "exclude_private",
				Type:       properties.FilterTypeExclude,
				Active:     true,
				Expression: `in_cidr(ip_address, "10.0.0.0/8")`,
			},
		},
	}
	modifier, err := NewFilter(config)
	require.NoError(t, err)

	session := &schema.Session{
		Events: []*schema.Event{
			{Values: map[string]any{"ip_address": "192.168.1.1"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "10.0.0.1"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "8.8.8.8"}, Metadata: make(map[string]any)},
			{Values: map[string]any{"ip_address": "1.1.1.1"}, Metadata: make(map[string]any)},
		},
	}

	// when
	sessions, err := modifier.Split(session)
	// then
	assert.NoError(t, err)
	assert.Len(t, sessions, 1)
	assert.Len(t, sessions[0].Events, 2)
	assert.Equal(t, "8.8.8.8", sessions[0].Events[0].Values["ip_address"])
	assert.Equal(t, "1.1.1.1", sessions[0].Events[1].Values["ip_address"])
}

func TestFilterModifierEmptySession(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"ip_address"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "test",
				Type:       properties.FilterTypeExclude,
				Active:     true,
				Expression: `ip_address != ""`,
			},
		},
	}
	modifier, err := NewFilter(config)
	require.NoError(t, err)

	session := &schema.Session{Events: []*schema.Event{}}

	// when
	sessions, err := modifier.Split(session)
	// then
	assert.NoError(t, err)
	assert.Len(t, sessions, 1)
	assert.Len(t, sessions[0].Events, 0)
}
