package protosessions

import (
	"context"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/stretchr/testify/assert"
)

// AtOffset wraps a hit with a time offset from test base time.
type AtOffset struct {
	Seconds int
	Hit     *hits.Hit
}

// ExpectedSession defines expected hits per session.
type ExpectedSession struct {
	ClientID string
	HitCount int
}

type testCase struct {
	name                  string
	hits                  []AtOffset
	evictionStrategy      EvictionStrategy
	settingsOpts          []properties.TestSettingsOption
	expectedProtoSessions []ExpectedSession
	expectedEvicted       int
}

func TestWorker_TableDriven(t *testing.T) {
	tests := []testCase{
		{
			name: "single_client_single_hit",
			hits: []AtOffset{
				{0, makeHit("client-a", ptr("stamp-a"), nil)},
			},
			expectedProtoSessions: []ExpectedSession{
				{ClientID: "client-a", HitCount: 1},
			},
			evictionStrategy: EvictWholeProtosessionStrategy,
		},
		{
			name: "two_clients_independent_sessions",
			hits: []AtOffset{
				{0, makeHit("client-a", ptr("stamp-a"), nil)},
				{1, makeHit("client-b", ptr("stamp-b"), nil)},
			},
			expectedProtoSessions: []ExpectedSession{
				{ClientID: "client-a", HitCount: 1},
				{ClientID: "client-b", HitCount: 1},
			},
			evictionStrategy: EvictWholeProtosessionStrategy,
		},
		{
			name: "session_stamp_conflict_evicts_later_client",
			hits: []AtOffset{
				{0, makeHit("client-a", ptr("shared-stamp"), nil)},
				{1, makeHit("client-b", ptr("shared-stamp"), nil)},
			},
			expectedProtoSessions: []ExpectedSession{
				{ClientID: "client-a", HitCount: 1},
			},
			expectedEvicted:  1,
			evictionStrategy: EvictWholeProtosessionStrategy,
		},
		{
			name: "session_stamp_conflict_with_rewrite_strategy",
			hits: []AtOffset{
				{0, makeHit("client-a", ptr("shared-stamp"), nil)},
				{1, makeHit("client-b", ptr("shared-stamp"), nil)},
			},
			evictionStrategy: RewriteIDAndUpdateInPlaceStrategy,
			expectedProtoSessions: []ExpectedSession{
				{ClientID: "client-a", HitCount: 2},
			},
			expectedEvicted: 0,
		},
		{
			name: "multiple_hits_same_client_accrue",
			hits: []AtOffset{
				{0, makeHit("client-a", ptr("stamp-a"), nil)},
				{5, makeHit("client-a", ptr("stamp-a"), nil)},
				{10, makeHit("client-a", ptr("stamp-a"), nil)},
			},
			expectedProtoSessions: []ExpectedSession{
				{ClientID: "client-a", HitCount: 3},
			},
			evictionStrategy: EvictWholeProtosessionStrategy,
		},
		{
			name: "user_id_joins_different_clients_no_session_stamp",
			hits: []AtOffset{
				{0, makeHit("client-a", nil, ptr("user-123"))},
				{1, makeHit("client-b", nil, ptr("user-123"))},
			},
			expectedProtoSessions: []ExpectedSession{
				{ClientID: "client-a", HitCount: 2},
			},
			evictionStrategy: RewriteIDAndUpdateInPlaceStrategy,
		},
		{
			name: "user_id_evicts_whole_proto_session 1",
			hits: []AtOffset{
				{0, makeHit("client-a", nil, ptr("user-123"))},
				{1, makeHit("client-a", nil, ptr("user-123"))},
				{2, makeHit("client-b", nil, ptr("user-123"))},
			},
			expectedProtoSessions: []ExpectedSession{
				{ClientID: "client-a", HitCount: 2},
			},
			expectedEvicted:  1,
			evictionStrategy: EvictWholeProtosessionStrategy,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// given
			baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
			results := make(map[hits.ClientID][]*hits.Hit)
			clashes := make(map[string]string)
			var evictions []*hits.Hit

			tickerBackend := NewGenericKVTimingWheelBackend("protosessions", storage.NewInMemoryKV())
			backend := NewTestBatchedIOBackend(
				WithAppendHitsHandler(func(req *AppendHitsToProtoSessionRequest) *AppendHitsToProtoSessionResponse {
					if results[req.ProtoSessionID] == nil {
						results[req.ProtoSessionID] = make([]*hits.Hit, 0)
					}
					results[req.ProtoSessionID] = append(results[req.ProtoSessionID], req.Hits...)
					return &AppendHitsToProtoSessionResponse{}
				}),
				WithIdentifierConflictHandler(func(req *IdentifierConflictRequest) *IdentifierConflictResponse {
					stamp := req.ExtractIdentifier(req.Hit)
					currentClient := string(req.Hit.AuthoritativeClientID)
					if existing, ok := clashes[stamp]; ok {
						if existing != currentClient {
							return &IdentifierConflictResponse{
								HasConflict:   true,
								ConflictsWith: hits.ClientID(existing),
							}
						}
						return &IdentifierConflictResponse{}
					}
					clashes[stamp] = currentClient
					return &IdentifierConflictResponse{}
				}),
			)

			closer := NewTestCloser()
			requeuer := receiver.NewTestStorage(func(h []*hits.Hit) error {
				evictions = append(evictions, h...)
				return nil
			})

			settingsRegistry := properties.NewTestSettingRegistry(tc.settingsOpts...)

			handler := Handler(
				context.Background(),
				backend,
				tickerBackend,
				closer,
				requeuer,
				settingsRegistry,
				WithEvictionStrategy(tc.evictionStrategy),
				WithIdentifierIsolationGuardFactory(NewNoIsolationGuardFactory()),
			)

			// when
			inputHits := applyOffsets(baseTime, tc.hits)
			err := handler(map[string]string{}, &hits.HitProcessingTask{Hits: inputHits})

			// then
			assert.Nil(t, err)
			assert.Len(t, results, len(tc.expectedProtoSessions))
			for _, exp := range tc.expectedProtoSessions {
				assert.Len(t, results[hits.ClientID(exp.ClientID)], exp.HitCount,
					"client %s should have %d hits", exp.ClientID, exp.HitCount)
			}
			assert.Len(t, evictions, tc.expectedEvicted)
		})
	}
}

func makeHit(clientID string, sessionStamp, userID *string) *hits.Hit {
	h := hits.New()
	h.ClientID = hits.ClientID(clientID)
	h.AuthoritativeClientID = hits.ClientID(clientID)
	if sessionStamp != nil {
		h.Request.QueryParams.Set("fss", *sessionStamp)
	}
	h.PropertyID = "test-property"
	h.UserID = userID
	return h
}

func applyOffsets(baseTime time.Time, wrapped []AtOffset) []*hits.Hit {
	result := make([]*hits.Hit, len(wrapped))
	for i, ao := range wrapped {
		ao.Hit.Request.ServerReceivedTime = baseTime.Add(time.Duration(ao.Seconds) * time.Second)
		result[i] = ao.Hit
	}
	return result
}

func ptr(s string) *string {
	return &s
}
