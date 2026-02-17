package splitter

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
)

type expectedSessions []int

func TestSplitter(t *testing.T) {
	cases := []struct {
		name       string
		session    *schema.Session
		conditions []Condition
		expected   []expectedSessions
	}{
		{
			name: "UTM campaign - Single event - no split",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
				},
			},
			conditions: []Condition{
				NewUTMCampaignCondition(),
			},
			expected: []expectedSessions{
				{0},
			},
		},
		{
			name: "UTM campaign - Multiple events - no campaign",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUTMCampaignCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2},
			},
		},
		{
			name: "UTM campaign - Multiple events - the same campaign",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
				},
			},
			conditions: []Condition{
				NewUTMCampaignCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2},
			},
		},
		{
			name: "UTM campaign - Multiple events - only one contains campaign",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUTMCampaignCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2},
			},
		},
		{
			name: "UTM campaign - Multiple events - different campaigns",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign2"),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUTMCampaignCondition(),
			},
			expected: []expectedSessions{
				{0, 1},
				{2, 3},
			},
		},
		{
			name: "UTM campaign - Multiple events - empty campaign",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", ""),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUTMCampaignCondition(),
			},
			expected: []expectedSessions{
				{0, 1},
				{2, 3},
			},
		},
		{
			name: "UTM campaign - Multiple events - nil campaign",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", nil),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUTMCampaignCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2, 3},
			},
		},
		{
			name: "User ID - Single event - no split",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user1"),
				},
			},
			conditions: []Condition{
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0},
			},
		},
		{
			name: "User ID - Multiple events - no user id",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2},
			},
		},
		{
			name: "User ID - Multiple events - the same user id",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user1"),
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user1"),
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user1"),
				},
			},
			conditions: []Condition{
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2},
			},
		},
		{
			name: "User ID - Multiple events - different user ids",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user1"),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user2"),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1},
				{2, 3},
			},
		},
		{
			name: "User ID - Multiple events - appears mid-session",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user2"),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2, 3},
			},
		},
		{
			name: "User ID - Multiple events - appears mid-session, then changes",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user2"),
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user3"),
				},
			},
			conditions: []Condition{
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2},
				{3},
			},
		},
		{
			name: "User ID - First has id, then nulls",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user1"),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2, 3},
			},
		},
		{
			name: "User ID - Explicit null in the middle",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user1"),
					schema.NewEvent(hits.New()),
					// This is equivalent of not having a user id, effective the same case
					// as the one above
					schema.NewEvent(hits.New()).WithValueKey("user_id", nil),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2, 3},
			},
		},
		{
			name: "User ID - Explicit empty value in the middle",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("user_id", "user1"),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()).WithValueKey("user_id", ""),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2, 3},
			},
		},
		{
			name: "MaxXEvents - Single event - no split",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewMaxXEventsCondition(3),
			},
			expected: []expectedSessions{
				{0},
			},
		},
		{
			name: "MaxXEvents - Events below threshold - no split",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewMaxXEventsCondition(3),
			},
			expected: []expectedSessions{
				{0, 1},
			},
		},
		{
			name: "MaxXEvents - Events at threshold - split",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewMaxXEventsCondition(3),
			},
			expected: []expectedSessions{
				{0, 1, 2},
				{3},
			},
		},
		{
			name: "MaxXEvents - Multiple splits",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewMaxXEventsCondition(2),
			},
			expected: []expectedSessions{
				{0, 1},
				{2, 3},
				{4, 5},
				{6},
			},
		},
		{
			name: "TimeSinceFirstEvent - Single event - no split",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
				},
			},
			conditions: []Condition{
				NewTimeSinceFirstEventCondition(5 * time.Minute),
			},
			expected: []expectedSessions{
				{0},
			},
		},
		{
			name: "TimeSinceFirstEvent - Events below threshold - no split",
			session: func() *schema.Session {
				baseTime := time.Now()
				hit1 := hits.New()
				hit1.Request.ServerReceivedTime = baseTime
				hit2 := hits.New()
				hit2.Request.ServerReceivedTime = baseTime.Add(2 * time.Minute)
				hit3 := hits.New()
				hit3.Request.ServerReceivedTime = baseTime.Add(4 * time.Minute)
				return &schema.Session{
					Events: []*schema.Event{
						schema.NewEvent(hit1),
						schema.NewEvent(hit2),
						schema.NewEvent(hit3),
					},
				}
			}(),
			conditions: []Condition{
				NewTimeSinceFirstEventCondition(5 * time.Minute),
			},
			expected: []expectedSessions{
				{0, 1, 2},
			},
		},
		{
			name: "TimeSinceFirstEvent - Events at threshold - split",
			session: func() *schema.Session {
				baseTime := time.Now()
				hit1 := hits.New()
				hit1.Request.ServerReceivedTime = baseTime
				hit2 := hits.New()
				hit2.Request.ServerReceivedTime = baseTime.Add(2 * time.Minute)
				hit3 := hits.New()
				hit3.Request.ServerReceivedTime = baseTime.Add(5 * time.Minute)
				hit4 := hits.New()
				hit4.Request.ServerReceivedTime = baseTime.Add(7 * time.Minute)
				return &schema.Session{
					Events: []*schema.Event{
						schema.NewEvent(hit1),
						schema.NewEvent(hit2),
						schema.NewEvent(hit3),
						schema.NewEvent(hit4),
					},
				}
			}(),
			conditions: []Condition{
				NewTimeSinceFirstEventCondition(5 * time.Minute),
			},
			expected: []expectedSessions{
				{0, 1},
				{2, 3},
			},
		},
		{
			name: "TimeSinceFirstEvent - Multiple splits",
			session: func() *schema.Session {
				baseTime := time.Now()
				hit1 := hits.New()
				hit1.Request.ServerReceivedTime = baseTime
				hit2 := hits.New()
				hit2.Request.ServerReceivedTime = baseTime.Add(5 * time.Minute)
				hit3 := hits.New()
				hit3.Request.ServerReceivedTime = baseTime.Add(10 * time.Minute)
				hit4 := hits.New()
				hit4.Request.ServerReceivedTime = baseTime.Add(15 * time.Minute)
				return &schema.Session{
					Events: []*schema.Event{
						schema.NewEvent(hit1),
						schema.NewEvent(hit2),
						schema.NewEvent(hit3),
						schema.NewEvent(hit4),
					},
				}
			}(),
			conditions: []Condition{
				NewTimeSinceFirstEventCondition(5 * time.Minute),
			},
			expected: []expectedSessions{
				{0},
				{1},
				{2},
				{3},
			},
		},
		{
			name: "Smoke - UTM campaign and MaxXEvents",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign2"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign2"),
				},
			},
			conditions: []Condition{
				NewUTMCampaignCondition(),
				NewMaxXEventsCondition(2),
			},
			expected: []expectedSessions{
				{0, 1},
				{2},
				{3, 4},
			},
		},
		{
			name: "Smoke - TimeSinceFirstEvent and UserID",
			session: func() *schema.Session {
				baseTime := time.Now()
				hit1 := hits.New()
				hit1.Request.ServerReceivedTime = baseTime
				hit2 := hits.New()
				hit2.Request.ServerReceivedTime = baseTime.Add(2 * time.Minute)
				hit3 := hits.New()
				hit3.Request.ServerReceivedTime = baseTime.Add(4 * time.Minute)
				hit4 := hits.New()
				hit4.Request.ServerReceivedTime = baseTime.Add(6 * time.Minute)
				return &schema.Session{
					Events: []*schema.Event{
						schema.NewEvent(hit1).WithValueKey("user_id", "user1"),
						schema.NewEvent(hit2).WithValueKey("user_id", "user1"),
						schema.NewEvent(hit3).WithValueKey("user_id", "user2"),
						schema.NewEvent(hit4).WithValueKey("user_id", "user2"),
					},
				}
			}(),
			conditions: []Condition{
				NewTimeSinceFirstEventCondition(5 * time.Minute),
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1},
				{2, 3},
			},
		},
		{
			name: "Smoke - All conditions combined",
			session: func() *schema.Session {
				baseTime := time.Now()
				hit1 := hits.New()
				hit1.Request.ServerReceivedTime = baseTime
				hit2 := hits.New()
				hit2.Request.ServerReceivedTime = baseTime.Add(1 * time.Minute)
				hit3 := hits.New()
				hit3.Request.ServerReceivedTime = baseTime.Add(2 * time.Minute)
				hit4 := hits.New()
				hit4.Request.ServerReceivedTime = baseTime.Add(3 * time.Minute)
				hit5 := hits.New()
				hit5.Request.ServerReceivedTime = baseTime.Add(6 * time.Minute)
				hit6 := hits.New()
				hit6.Request.ServerReceivedTime = baseTime.Add(7 * time.Minute)
				return &schema.Session{
					Events: []*schema.Event{
						schema.NewEvent(hit1).WithValueKey("utm_campaign", "campaign1").WithValueKey("user_id", "user1"),
						schema.NewEvent(hit2).WithValueKey("utm_campaign", "campaign1").WithValueKey("user_id", "user1"),
						schema.NewEvent(hit3).WithValueKey("utm_campaign", "campaign1").WithValueKey("user_id", "user1"),
						schema.NewEvent(hit4).WithValueKey("utm_campaign", "campaign2").WithValueKey("user_id", "user1"),
						schema.NewEvent(hit5).WithValueKey("utm_campaign", "campaign2").WithValueKey("user_id", "user2"),
						schema.NewEvent(hit6).WithValueKey("utm_campaign", "campaign2").WithValueKey("user_id", "user2"),
					},
				}
			}(),
			conditions: []Condition{
				NewTimeSinceFirstEventCondition(5 * time.Minute),
				NewMaxXEventsCondition(3),
				NewUTMCampaignCondition(),
				NewUserIDCondition(),
			},
			expected: []expectedSessions{
				{0, 1, 2},
				{3},
				{4, 5},
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// given
			splitter := New(tt.conditions...)

			// when
			actual, err := splitter.Split(tt.session)
			assert.NoError(t, err)

			// then
			assert.Equal(t, len(tt.expected), len(actual))
			for i, expectedSession := range tt.expected {
				actualSession := actual[i]
				assert.Equal(t, len(expectedSession), len(actualSession.Events))

				for j, expectedIndex := range expectedSession {
					expectedEventID := tt.session.Events[expectedIndex].BoundHit.ID
					actualEventID := actualSession.Events[j].BoundHit.ID
					assert.Equal(t, expectedEventID, actualEventID)
				}
			}
		})
	}
}

func TestAssignsSplitCauseToFirstEventOfNewSession(t *testing.T) {
	// given
	splitter := New(NewUTMCampaignCondition())
	session := &schema.Session{
		Events: []*schema.Event{
			schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
			schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign2"),
		},
	}

	// when
	actual, err := splitter.Split(session)

	// then
	assert.NoError(t, err)
	assert.Equal(t, nil, actual[0].Events[0].Metadata["session_split_cause"])
	assert.Equal(t, SplitCauseUtmCampaignChange, actual[1].Events[0].Metadata["session_split_cause"])
}

func TestMultiModifier(t *testing.T) {
	cases := []struct {
		name      string
		session   *schema.Session
		modifiers []SessionModifier
		expected  []expectedSessions
	}{
		{
			name: "Two modifiers in sequence",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign2"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign2"),
				},
			},
			modifiers: []SessionModifier{
				New(NewUTMCampaignCondition()),
				New(NewMaxXEventsCondition(1)),
			},
			expected: []expectedSessions{
				{0},
				{1},
				{2},
				{3},
			},
		},
		{
			name: "Noop modifier in chain",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
					schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign2"),
				},
			},
			modifiers: []SessionModifier{
				NewNoop(),
				New(NewUTMCampaignCondition()),
			},
			expected: []expectedSessions{
				{0},
				{1},
			},
		},
		{
			name: "Multiple modifiers with no splits",
			session: &schema.Session{
				Events: []*schema.Event{
					schema.NewEvent(hits.New()),
					schema.NewEvent(hits.New()),
				},
			},
			modifiers: []SessionModifier{
				NewNoop(),
				NewNoop(),
			},
			expected: []expectedSessions{
				{0, 1},
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// given
			multi := NewMultiModifier(tt.modifiers...)

			// when
			actual, err := multi.Split(tt.session)
			assert.NoError(t, err)

			// then
			assert.Equal(t, len(tt.expected), len(actual))
			for i, expectedSession := range tt.expected {
				actualSession := actual[i]
				assert.Equal(t, len(expectedSession), len(actualSession.Events))

				for j, expectedIndex := range expectedSession {
					expectedEventID := tt.session.Events[expectedIndex].BoundHit.ID
					actualEventID := actualSession.Events[j].BoundHit.ID
					assert.Equal(t, expectedEventID, actualEventID)
				}
			}
		})
	}
}

func TestMultiModifierEmptySession(t *testing.T) {
	// given
	multi := NewMultiModifier(
		NewNoop(),
		New(NewUTMCampaignCondition()),
	)
	session := &schema.Session{Events: []*schema.Event{}}

	// when
	actual, err := multi.Split(session)

	// then
	assert.NoError(t, err)
	assert.Len(t, actual, 1)
	assert.Len(t, actual[0].Events, 0)
}

func TestChainedRegistry(t *testing.T) {
	// given - a base registry that returns a simple splitter
	baseSplitter := New(NewUTMCampaignCondition())
	baseRegistry := NewStaticRegistry(baseSplitter)

	// Create a prepend modifier
	prependModifier := NewNoop()

	// Create chained registry
	chainedReg := NewChainedRegistry(baseRegistry, prependModifier)

	session := &schema.Session{
		Events: []*schema.Event{
			schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign1"),
			schema.NewEvent(hits.New()).WithValueKey("utm_campaign", "campaign2"),
		},
	}

	// when
	splitter, err := chainedReg.Splitter("any-property")
	assert.NoError(t, err)

	actual, err := splitter.Split(session)

	// then
	assert.NoError(t, err)
	// Should split due to UTM campaign change
	assert.Equal(t, 2, len(actual))
}
