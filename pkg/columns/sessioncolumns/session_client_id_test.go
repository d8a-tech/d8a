package sessioncolumns

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
)

func TestSessionClientIDColumn_Write(t *testing.T) {
	assert.True(t, SessionClientIDColumn.Implements().Field.Nullable)

	tests := []struct {
		name             string
		session          *schema.Session
		expectedClientID string
		expectedError    string
		expectedPanic    bool
	}{
		{
			name: "normal session writes first event client ID",
			session: &schema.Session{
				Events: []*schema.Event{
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("first-client-id")}},
					{BoundHit: &hits.Hit{ClientID: hits.ClientID("second-client-id")}},
				},
				Values: map[string]any{},
			},
			expectedClientID: "first-client-id",
		},
		{
			name: "empty session returns broken session error",
			session: &schema.Session{
				Events: []*schema.Event{},
				Values: map[string]any{},
			},
			expectedError: "session has no events",
		},
		{
			name: "missing BoundHit panics consistently with nearby columns",
			session: &schema.Session{
				Events: []*schema.Event{{BoundHit: nil}},
				Values: map[string]any{},
			},
			expectedPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			session := tt.session

			if tt.expectedPanic {
				// when / then
				assert.Panics(t, func() {
					_ = SessionClientIDColumn.Write(session)
				})
				return
			}

			// when
			err := SessionClientIDColumn.Write(session)

			// then
			if tt.expectedError != "" {
				assert.EqualError(t, err, tt.expectedError)
				assert.IsType(t, &schema.BrokenSessionError{}, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedClientID, session.Values[columns.CoreInterfaces.SessionClientID.Field.Name])
		})
	}
}
