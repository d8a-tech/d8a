package columns

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
)

func TestCastToBool(t *testing.T) {
	// given
	columnID := schema.InterfaceID("test_column")
	castFunc := CastToBool(columnID)

	tests := []struct {
		name      string
		input     any
		expected  bool
		expectErr bool
	}{
		// Boolean inputs
		{"direct true", true, true, false},
		{"direct false", false, false, false},

		// Truthy string values
		{"string true", "true", true, false},
		{"string TRUE", "TRUE", true, false},
		{"string yes", "yes", true, false},
		{"string YES", "YES", true, false},
		{"string y", "y", true, false},
		{"string Y", "Y", true, false},
		{"string on", "on", true, false},
		{"string ON", "ON", true, false},
		{"string 1", "1", true, false},
		{"string t", "t", true, false},
		{"string T", "T", true, false},

		// Falsy string values
		{"string false", "false", false, false},
		{"string FALSE", "FALSE", false, false},
		{"string no", "no", false, false},
		{"string NO", "NO", false, false},
		{"string n", "n", false, false},
		{"string N", "N", false, false},
		{"string off", "off", false, false},
		{"string OFF", "OFF", false, false},
		{"string 0", "0", false, false},
		{"string f", "f", false, false},
		{"string F", "F", false, false},
		{"empty string", "", false, true},

		// Whitespace handling
		{"whitespace true", "  true  ", true, false},
		{"whitespace false", "  false  ", false, false},
		{"whitespace yes", "\tyes\n", true, false},

		// Unrecognized values (should return error)
		{"unrecognized string", "maybe", false, true},
		{"number 2", "2", false, true},
		{"random text", "random", false, true},

		// Non-string, non-boolean values (should return false)
		{"integer 1", 1, false, false},
		{"integer 0", 0, false, false},
		{"nil value", nil, false, false},
		{"slice", []string{"test"}, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			result, err := castFunc(tt.input)

			// then
			if tt.expectErr {
				assert.Error(t, err)
				assert.Equal(t, tt.expected, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFirstEventValueFromMostFrequentClientIDColumn(t *testing.T) {
	columnID := schema.InterfaceID("test_session_column")
	field := &arrow.Field{Name: "test_session_field", Type: arrow.BinaryTypes.String, Nullable: true}

	tests := []struct {
		name          string
		session       *schema.Session
		extract       func(*schema.Event) (any, schema.D8AColumnWriteError)
		expectedValue any
		expectedError string
	}{
		{
			name: "empty session returns broken session error",
			session: &schema.Session{
				Events: []*schema.Event{},
				Values: map[string]any{},
			},
			extract:       ExctractFieldValue("target"),
			expectedError: "session has no events",
		},
		{
			name: "single client ID returns value from first event",
			session: &schema.Session{
				Events: []*schema.Event{
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("client-a")}, Values: map[string]any{"target": "first-value"}},
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("client-a")}, Values: map[string]any{"target": "later-value"}},
				},
				Values: map[string]any{},
			},
			extract:       ExctractFieldValue("target"),
			expectedValue: "first-value",
		},
		{
			name: "dominant later client ID returns first event from that client ID",
			session: &schema.Session{
				Events: []*schema.Event{
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("client-a")}, Values: map[string]any{"target": "a-first"}},
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("client-b")}, Values: map[string]any{"target": "b-first"}},
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("client-b")}, Values: map[string]any{"target": "b-second"}},
				},
				Values: map[string]any{},
			},
			extract:       ExctractFieldValue("target"),
			expectedValue: "b-first",
		},
		{
			name: "tied counts choose earliest first appearance and allow nil value",
			session: &schema.Session{
				Events: []*schema.Event{
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("client-a")}, Values: map[string]any{}},
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("client-b")}, Values: map[string]any{"target": "b-first"}},
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("client-a")}, Values: map[string]any{"target": "a-second"}},
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("client-b")}, Values: map[string]any{"target": "b-second"}},
				},
				Values: map[string]any{},
			},
			extract:       ExctractFieldValue("target"),
			expectedValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			column := FirstEventValueFromMostFrequentClientIDColumn(columnID, field, tt.extract)

			err := column.Write(tt.session)

			if tt.expectedError != "" {
				assert.EqualError(t, err, tt.expectedError)
				assert.IsType(t, &schema.BrokenSessionError{}, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedValue, tt.session.Values[field.Name])
		})
	}
}
