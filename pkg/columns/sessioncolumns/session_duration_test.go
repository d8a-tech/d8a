package sessioncolumns

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
)

func TestSessionDurationColumn(t *testing.T) {
	tests := []struct {
		name          string
		session       *schema.Session
		expectedError bool
		expected      int64
	}{
		{
			name: "basic duration calculation",
			session: &schema.Session{
				PropertyID: "1",
				Metadata:   map[string]any{},
				Values: map[string]any{
					columns.CoreInterfaces.SessionFirstEventTime.Field.Name: int64(1609459200),
					columns.CoreInterfaces.SessionLastEventTime.Field.Name:  int64(1609459201),
				},
			},
			expectedError: false,
			expected:      int64(1),
		},
		{
			name: "no prereqs met",
			session: &schema.Session{
				PropertyID: "1",
				Metadata:   map[string]any{},
			},
			expectedError: true,
			expected:      0,
		},
		{
			name: "no last event time",
			session: &schema.Session{
				PropertyID: "1",
				Metadata:   map[string]any{},
				Values: map[string]any{
					columns.CoreInterfaces.SessionFirstEventTime.Field.Name: int64(1609459200),
				},
			},
			expectedError: true,
			expected:      0,
		},
		{
			name: "no first event time",
			session: &schema.Session{
				PropertyID: "1",
				Metadata:   map[string]any{},
				Values: map[string]any{
					columns.CoreInterfaces.SessionLastEventTime.Field.Name: int64(1609459201),
				},
			},
			expectedError: true,
			expected:      0,
		},
		{
			name: "last earlier than first",
			session: &schema.Session{
				PropertyID: "1",
				Metadata:   map[string]any{},
				Values: map[string]any{
					columns.CoreInterfaces.SessionFirstEventTime.Field.Name: int64(1609459201),
					columns.CoreInterfaces.SessionLastEventTime.Field.Name:  int64(1609459200),
				},
			},
			expectedError: true,
			expected:      0,
		},
		{
			name: "first not int64",
			session: &schema.Session{
				PropertyID: "1",
				Metadata:   map[string]any{},
				Values: map[string]any{
					columns.CoreInterfaces.SessionFirstEventTime.Field.Name: "not an int64",
					columns.CoreInterfaces.SessionLastEventTime.Field.Name:  int64(1609459201),
				},
			},
			expectedError: true,
			expected:      0,
		},
		{
			name: "last not int64",
			session: &schema.Session{
				PropertyID: "1",
				Metadata:   map[string]any{},
				Values: map[string]any{
					columns.CoreInterfaces.SessionFirstEventTime.Field.Name: int64(1609459200),
					columns.CoreInterfaces.SessionLastEventTime.Field.Name:  "not an int64",
				},
			},
			expectedError: true,
			expected:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			session := tt.session

			// when
			err := DurationColumn.Write(session)

			// then
			assert.Equal(t, tt.expectedError, err != nil)
			if !tt.expectedError {
				assert.Equal(t, tt.expected, session.Values["session_duration"])
			}
		})
	}
}
