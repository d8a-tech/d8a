package sessions

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSessionWriter is a test mock for SessionWriter
type mockSessionWriter struct {
	writeCalled bool
	writeError  error
	sessions    []*schema.Session
}

func (m *mockSessionWriter) Write(sessions ...*schema.Session) error {
	m.writeCalled = true
	m.sessions = append(m.sessions, sessions...)
	return m.writeError
}

// Compile-time check to ensure mockSessionWriter implements SessionWriterInterface
var _ SessionWriter = (*mockSessionWriter)(nil)

func TestDirectCloser_Close(t *testing.T) {
	// Test times in RFC3339 format
	time1 := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	time2 := time.Date(2023, 1, 1, 11, 0, 0, 0, time.UTC)
	time3 := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	time4 := time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		protosession  []*hits.Hit
		expectedOrder []time.Time // expected order of ServerReceivedTime
		writerError   error
		expectedError bool
		expectedCalls int
	}{
		{
			name: "events sorted by server received time - mixed order",
			protosession: []*hits.Hit{
				{ID: "hit3", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time3}},
				{ID: "hit1", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time1}},
				{ID: "hit4", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time4}},
				{ID: "hit2", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time2}},
			},
			expectedOrder: []time.Time{time1, time2, time3, time4},
			expectedCalls: 1,
		},
		{
			name: "events already in correct order",
			protosession: []*hits.Hit{
				{ID: "hit1", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time1}},
				{ID: "hit2", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time2}},
				{ID: "hit3", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time3}},
			},
			expectedOrder: []time.Time{time1, time2, time3},
			expectedCalls: 1,
		},
		{
			name: "events in reverse order",
			protosession: []*hits.Hit{
				{ID: "hit3", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time3}},
				{ID: "hit2", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time2}},
				{ID: "hit1", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time1}},
			},
			expectedOrder: []time.Time{time1, time2, time3},
			expectedCalls: 1,
		},
		{
			name: "single event",
			protosession: []*hits.Hit{
				{ID: "hit1", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time1}},
			},
			expectedOrder: []time.Time{time1},
			expectedCalls: 1,
		},
		{
			name:          "empty protosession",
			protosession:  []*hits.Hit{},
			expectedOrder: []time.Time{},
			expectedCalls: 0,
		},
		{
			name: "events with same timestamp",
			protosession: []*hits.Hit{
				{ID: "hit1", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time1}},
				{ID: "hit2", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time1}},
				{ID: "hit3", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time2}},
			},
			expectedOrder: []time.Time{time1, time1, time2},
			expectedCalls: 1,
		},
		{
			name: "events with invalid time format - should not crash",
			protosession: []*hits.Hit{
				{ID: "hit1", PropertyID: "prop1",
					Request: &hits.Request{ServerReceivedTime: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)}},
				{ID: "hit2", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time2}},
				{ID: "hit3", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time1}},
			},
			// Invalid times should maintain original order relative to each other
			// Valid times should be sorted
			expectedOrder: []time.Time{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time1, time2},
			expectedCalls: 1,
		},
		{
			name: "writer returns error",
			protosession: []*hits.Hit{
				{ID: "hit1", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: time1}},
			},
			expectedOrder: []time.Time{time1},
			writerError:   assert.AnError,
			expectedError: true,
			expectedCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			mockWriter := &mockSessionWriter{
				writeError: tt.writerError,
			}
			closer := NewDirectCloser(mockWriter, 0)

			// when
			err := closer.Close([][]*hits.Hit{tt.protosession})

			// then
			if tt.expectedError {
				assert.Error(t, err)
				assert.Equal(t, tt.writerError, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectedCalls > 0, mockWriter.writeCalled)
			if mockWriter.writeCalled {
				require.Len(t, mockWriter.sessions, 1)
				session := mockWriter.sessions[0]

				// Verify session structure
				assert.Equal(t, len(tt.protosession), len(session.Events))
				assert.Equal(t, tt.protosession[0].PropertyID, session.PropertyID)
				assert.NotNil(t, session.Values)

				// Verify events are sorted by ServerReceivedTime
				actualOrder := make([]time.Time, len(session.Events))
				for i, event := range session.Events {
					actualOrder[i] = event.BoundHit.MustServerAttributes().ServerReceivedTime
				}
				assert.Equal(t, tt.expectedOrder, actualOrder)

				// Verify event structure
				for i, event := range session.Events {
					assert.NotNil(t, event.BoundHit)
					assert.NotNil(t, event.Values)
					assert.Equal(t, tt.expectedOrder[i], event.BoundHit.MustServerAttributes().ServerReceivedTime)
				}
			} else {
				// If no writer call was expected, verify no sessions were written
				assert.Empty(t, mockWriter.sessions)
			}
		})
	}
}

func TestDirectCloser_Close_EmptyProtosession(t *testing.T) {
	// given
	mockWriter := &mockSessionWriter{}
	closer := NewDirectCloser(mockWriter, 0*time.Second)
	var protosession []*hits.Hit

	// when
	err := closer.Close([][]*hits.Hit{protosession})

	// then
	assert.NoError(t, err)
	assert.False(t, mockWriter.writeCalled)
}

func TestDirectCloser_SortingStability(t *testing.T) {
	// Test that the sorting is stable for events with same timestamps
	baseTime := time.Now()

	// given
	protosession := []*hits.Hit{
		{ID: "first", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: baseTime}},
		{ID: "second", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: baseTime}},
		{ID: "third", PropertyID: "prop1", Request: &hits.Request{ServerReceivedTime: baseTime}},
	}

	mockWriter := &mockSessionWriter{}
	closer := NewDirectCloser(mockWriter, 0*time.Second)

	// when
	err := closer.Close([][]*hits.Hit{protosession})

	// then
	assert.NoError(t, err)
	require.True(t, mockWriter.writeCalled)
	require.Len(t, mockWriter.sessions, 1)

	session := mockWriter.sessions[0]
	require.Len(t, session.Events, 3)

	// All events should have the same timestamp
	for _, event := range session.Events {
		assert.Equal(t, baseTime, event.BoundHit.MustServerAttributes().ServerReceivedTime)
	}
}

func TestNewDirectCloser(t *testing.T) {
	// given
	mockWriter := &mockSessionWriter{}

	// when
	closer := NewDirectCloser(mockWriter, 0*time.Second)

	// then
	assert.NotNil(t, closer)
	assert.Equal(t, mockWriter, closer.writer)
}
