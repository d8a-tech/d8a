package splitter

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCustomFunctions(t *testing.T) {
	// Tests each expression capability via the full interpreter pipeline (NewFilter + Split),
	// matching how they are used in production.
	tests := []struct {
		name       string
		field      string
		fieldValue string
		expression string
		wantMatch  bool // true means the exclude condition fires and the event is dropped
	}{
		// startsWith operator
		{
			name:       "startsWith matches",
			field:      "ip_address",
			fieldValue: "192.168.1.1",
			expression: `ip_address startsWith "192.168"`,
			wantMatch:  true,
		},
		{
			name:       "startsWith no match",
			field:      "ip_address",
			fieldValue: "10.0.0.1",
			expression: `ip_address startsWith "192.168"`,
			wantMatch:  false,
		},
		// endsWith operator
		{
			name:       "endsWith no match",
			field:      "hostname",
			fieldValue: "device.example.com",
			expression: `hostname endsWith ".100"`,
			wantMatch:  false,
		},
		{
			name:       "endsWith matches",
			field:      "hostname",
			fieldValue: "192.168.1.100",
			expression: `hostname endsWith ".100"`,
			wantMatch:  true,
		},
		// matches operator (regex)
		{
			name:       "matches operator matches",
			field:      "ip_address",
			fieldValue: "10.0.0.5",
			expression: `ip_address matches "^10\\.0\\.0\\.[0-9]{1,3}$"`,
			wantMatch:  true,
		},
		{
			name:       "matches operator no match",
			field:      "ip_address",
			fieldValue: "10.0.0.30",
			expression: `ip_address matches "^10\\.0\\.0\\.(1[0-9]|2[0-5])$"`,
			wantMatch:  false,
		},
		// inCidr – basic cases
		{
			name:       "inCidr matches IPv4",
			field:      "ip_address",
			fieldValue: "192.168.1.50",
			expression: `inCidr(ip_address, "192.168.0.0/16")`,
			wantMatch:  true,
		},
		{
			name:       "inCidr no match IPv4",
			field:      "ip_address",
			fieldValue: "10.0.0.1",
			expression: `inCidr(ip_address, "192.168.0.0/16")`,
			wantMatch:  false,
		},
		{
			name:       "inCidr matches private",
			field:      "ip_address",
			fieldValue: "10.5.0.1",
			expression: `inCidr(ip_address, "10.0.0.0/8")`,
			wantMatch:  true,
		},
		// inCidr – edge cases
		{
			name:       "inCidr boundary IP in /10 CIDR",
			field:      "ip_address",
			fieldValue: "100.64.0.0",
			expression: `inCidr(ip_address, "100.64.0.0/10")`,
			wantMatch:  true,
		},
		{
			name:       "inCidr last IP in /10 CIDR",
			field:      "ip_address",
			fieldValue: "100.127.255.255",
			expression: `inCidr(ip_address, "100.64.0.0/10")`,
			wantMatch:  true,
		},
		{
			name:       "inCidr just outside /10 CIDR",
			field:      "ip_address",
			fieldValue: "100.128.0.0",
			expression: `inCidr(ip_address, "100.64.0.0/10")`,
			wantMatch:  false,
		},
		{
			name:       "inCidr IPv6 in CIDR",
			field:      "ip_address",
			fieldValue: "2001:db8::1",
			expression: `inCidr(ip_address, "2001:db8::/32")`,
			wantMatch:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			config := properties.FiltersConfig{
				Fields: []string{tt.field},
				Conditions: []properties.ConditionConfig{
					{
						Name:       "test_condition",
						Type:       properties.FilterTypeExclude,
						TestMode:   false,
						Expression: tt.expression,
					},
				},
			}
			modifier, err := NewFilter(config)
			require.NoError(t, err)

			session := &schema.Session{
				Events: []*schema.Event{
					{Values: map[string]any{tt.field: tt.fieldValue}, Metadata: make(map[string]any)},
				},
			}

			// when
			sessions, err := modifier.Split(session)

			// then
			require.NoError(t, err)
			if tt.wantMatch {
				// exclude condition fired: event dropped, session empty
				assert.Len(t, sessions, 0, "expected event to be excluded")
			} else {
				// exclude condition did not fire: event kept
				assert.Len(t, sessions, 1, "expected event to be kept")
				assert.Len(t, sessions[0].Events, 1)
			}
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
				TestMode:   false,
				Expression: `ip_address startsWith "192.168"`,
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
				TestMode:   false,
				Expression: `inCidr(ip_address, "100.64.0.0/10")`,
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
				TestMode:   true,
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
				TestMode:   false,
				Expression: `ip_address startsWith "192.168" || inCidr(ip_address, "10.0.0.0/8")`,
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
				TestMode:   false,
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
				TestMode:   false,
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
				TestMode:   false,
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

func TestFilterModifierMultipleConditions(t *testing.T) {
	// given
	config := properties.FiltersConfig{
		Fields: []string{"ip_address"},
		Conditions: []properties.ConditionConfig{
			{
				Name:       "exclude_internal",
				Type:       properties.FilterTypeExclude,
				TestMode:   false,
				Expression: `ip_address startsWith "192.168"`,
			},
			{
				Name:       "exclude_private",
				Type:       properties.FilterTypeExclude,
				TestMode:   false,
				Expression: `inCidr(ip_address, "10.0.0.0/8")`,
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
				TestMode:   false,
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
