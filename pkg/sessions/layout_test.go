package sessions

import (
	"testing"

	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockLayout is a mock implementation of schema.Layout
type mockLayout struct {
	mock.Mock
}

func (m *mockLayout) Tables(columns schema.Columns) []schema.WithName {
	args := m.Called(columns)
	res, ok := args.Get(0).([]schema.WithName)
	if !ok {
		return nil
	}
	return res
}

func (m *mockLayout) ToRows(columns schema.Columns, sessions ...*schema.Session) ([]schema.TableRows, error) {
	args := m.Called(columns, sessions)
	res, ok := args.Get(0).([]schema.TableRows)
	if !ok {
		return nil, args.Error(1)
	}
	return res, args.Error(1)
}

func TestBrokenFilteringLayout_ToRows(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logrus.StandardLogger().ReplaceHooks(make(logrus.LevelHooks))
	logrus.AddHook(hook)
	logrus.SetLevel(logrus.WarnLevel)
	logrus.SetOutput(logger.Out)

	tests := []struct {
		name           string
		sessions       []*schema.Session
		expectedPassed []*schema.Session
		expectedLogs   []string
		expectedRows   []schema.TableRows
		expectToRows   bool
	}{
		{
			name: "basic filtering - mix of broken and non-broken events",
			sessions: []*schema.Session{
				{
					Events: []*schema.Event{
						{IsBroken: false},
						{IsBroken: true, BrokenReason: "event1 broken"},
						{IsBroken: false},
					},
				},
			},
			expectedPassed: []*schema.Session{
				{
					Events: []*schema.Event{
						{IsBroken: false},
						{IsBroken: false},
					},
				},
			},
			expectedLogs: []string{"skipping write for broken event: event1 broken"},
			expectedRows: []schema.TableRows{{Table: "test", Rows: []map[string]any{{"key": "val"}}}},
			expectToRows: true,
		},
		{
			name: "all broken events - session should be skipped",
			sessions: []*schema.Session{
				{
					Events: []*schema.Event{
						{IsBroken: true, BrokenReason: "all events broken"},
					},
				},
			},
			expectedPassed: []*schema.Session{},
			expectedLogs:   []string{"skipping write for broken event: all events broken"},
			expectedRows:   []schema.TableRows{},
			expectToRows:   true,
		},
		{
			name: "none broken - everything passed",
			sessions: []*schema.Session{
				{
					Events: []*schema.Event{
						{IsBroken: false},
						{IsBroken: false},
					},
				},
			},
			expectedPassed: []*schema.Session{
				{
					Events: []*schema.Event{
						{IsBroken: false},
						{IsBroken: false},
					},
				},
			},
			expectedLogs: []string{},
			expectedRows: []schema.TableRows{{Table: "test", Rows: []map[string]any{{"key": "val"}}}},
			expectToRows: true,
		},
		{
			name: "broken session - skipped entirely",
			sessions: []*schema.Session{
				{
					IsBroken:     true,
					BrokenReason: "session is broken",
					Events: []*schema.Event{
						{IsBroken: false},
					},
				},
			},
			expectedPassed: []*schema.Session{},
			expectedLogs:   []string{"skipping write for broken session: session is broken"},
			expectedRows:   []schema.TableRows{},
			expectToRows:   true,
		},
		{
			name: "multiple sessions - mixed scenarios",
			sessions: []*schema.Session{
				{
					PropertyID: "s1",
					Events: []*schema.Event{
						{IsBroken: false},
						{IsBroken: true, BrokenReason: "s1e2 broken"},
					},
				},
				{
					PropertyID:   "s2",
					IsBroken:     true,
					BrokenReason: "s2 broken",
				},
				{
					PropertyID: "s3",
					Events: []*schema.Event{
						{IsBroken: false},
					},
				},
			},
			expectedPassed: []*schema.Session{
				{
					PropertyID: "s1",
					Events: []*schema.Event{
						{IsBroken: false},
					},
				},
				{
					PropertyID: "s3",
					Events: []*schema.Event{
						{IsBroken: false},
					},
				},
			},
			expectedLogs: []string{
				"skipping write for broken event: s1e2 broken",
				"skipping write for broken session: s2 broken",
			},
			expectedRows: []schema.TableRows{{Table: "test", Rows: []map[string]any{{"key": "val"}}}},
			expectToRows: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			hook.Reset()
			innerLayout := new(mockLayout)
			layout := &brokenFilteringLayout{layout: innerLayout}
			cols := schema.Columns{}

			if tt.expectToRows {
				innerLayout.On("ToRows", cols, mock.MatchedBy(func(passedSessions []*schema.Session) bool {
					if len(passedSessions) != len(tt.expectedPassed) {
						return false
					}
					for i := range passedSessions {
						if len(passedSessions[i].Events) != len(tt.expectedPassed[i].Events) {
							return false
						}
						if passedSessions[i].PropertyID != tt.expectedPassed[i].PropertyID {
							return false
						}
					}
					return true
				})).Return(tt.expectedRows, nil)
			}

			// when
			rows, err := layout.ToRows(cols, tt.sessions...)

			// then
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, rows)

			// verify logs
			assert.Len(t, hook.Entries, len(tt.expectedLogs))
			for i, expectedMsg := range tt.expectedLogs {
				assert.Equal(t, expectedMsg, hook.Entries[i].Message)
				assert.Equal(t, logrus.WarnLevel, hook.Entries[i].Level)
			}

			innerLayout.AssertExpectations(t)
		})
	}
}

func TestBrokenFilteringLayout_InPlaceFiltering(t *testing.T) {
	// given
	innerLayout := new(mockLayout)
	layout := &brokenFilteringLayout{layout: innerLayout}
	cols := schema.Columns{}

	e1 := &schema.Event{IsBroken: false}
	e2 := &schema.Event{IsBroken: true, BrokenReason: "broken"}
	e3 := &schema.Event{IsBroken: false}

	events := []*schema.Event{e1, e2, e3}
	session := &schema.Session{
		Events: events,
	}

	innerLayout.On("ToRows", cols, mock.Anything).Return([]schema.TableRows{}, nil)

	// when
	_, _ = layout.ToRows(cols, session)

	// then
	// Verify in-place modification: the Events slice of the session should now contain only e1 and e3
	assert.Len(t, session.Events, 2)
	assert.Equal(t, e1, session.Events[0])
	assert.Equal(t, e3, session.Events[1])

	// Verify that the underlying array was reused
	// We check if the address of the first element of the slice is the same as the original
	assert.Same(t, e1, session.Events[0])
	assert.Same(t, e3, session.Events[1])

	// Check if the original events slice was modified in place
	assert.Equal(t, e1, events[0])
	assert.Equal(t, e3, events[1])
}
