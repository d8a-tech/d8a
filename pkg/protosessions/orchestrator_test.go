package protosessions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/stretchr/testify/assert"
)

type processBucketCase struct {
	name               string
	protoSessions      [][]*hits.Hit
	fetchErr           error
	closeErr           error
	expectClosedCount  int
	expectCleanedCount int
	expectError        bool
}

func TestOrchestrator_ProcessBucket(t *testing.T) {
	tests := []processBucketCase{
		{
			name:               "empty_bucket_advances",
			protoSessions:      [][]*hits.Hit{},
			expectClosedCount:  0,
			expectCleanedCount: 0,
		},
		{
			name: "single_protosession_closes_and_cleans",
			protoSessions: [][]*hits.Hit{
				{makeTimedHit("c1", 100), makeTimedHit("c1", 200)},
			},
			expectClosedCount:  1,
			expectCleanedCount: 1,
		},
		{
			name: "multiple_protosessions_batch_close",
			protoSessions: [][]*hits.Hit{
				{makeTimedHit("c1", 100)},
				{makeTimedHit("c2", 200)},
			},
			expectClosedCount:  2,
			expectCleanedCount: 2,
		},
		{
			name:              "fetch_error_propagates",
			fetchErr:          errors.New("backend down"),
			expectClosedCount: 0,
			expectError:       true,
		},
		{
			name: "closer_error_propagates",
			protoSessions: [][]*hits.Hit{
				{makeTimedHit("c1", 100)},
			},
			closeErr:          errors.New("write failed"),
			expectClosedCount: 0,
			expectError:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// given
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var closedSessions [][]*hits.Hit
			var cleanedIDs []hits.ClientID

			tickerBackend := NewGenericKVTimingWheelBackend("protosessions", storage.NewInMemoryKV())
			backend := NewTestBatchedIOBackend(
				WithGetAllProtosessionsForBucketHandler(
					func(_ *GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse {
						return &GetAllProtosessionsForBucketResponse{
							ProtoSessions: tc.protoSessions,
							Err:           tc.fetchErr,
						}
					}),
				WithRemoveProtoSessionHitsHandler(
					func(req *RemoveProtoSessionHitsRequest) *RemoveProtoSessionHitsResponse {
						cleanedIDs = append(cleanedIDs, req.ProtoSessionID)
						return &RemoveProtoSessionHitsResponse{}
					}),
			)

			closer := NewTestCloser(WithCloseHandler(func(sessions [][]*hits.Hit) error {
				if tc.closeErr != nil {
					return tc.closeErr
				}
				closedSessions = append(closedSessions, sessions...)
				return nil
			}))

			requeuer := receiver.NewTestStorage(nil)
			settingsRegistry := properties.NewTestSettingRegistry()

			orchestrator := NewOrchestrator(
				ctx,
				backend,
				tickerBackend,
				closer,
				requeuer,
				settingsRegistry,
			)

			// when
			err := orchestrator.processBucket(ctx, 100)

			// then
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, closedSessions, tc.expectClosedCount)
				assert.Len(t, cleanedIDs, tc.expectCleanedCount)
			}
		})
	}
}

func TestOrchestrator_ProcessBucket_SortsHitsByTime(t *testing.T) {
	// given
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	// Hits intentionally out of order
	unsortedHits := []*hits.Hit{
		makeTimedHitAt("c1", baseTime.Add(20*time.Second)),
		makeTimedHitAt("c1", baseTime.Add(5*time.Second)),
		makeTimedHitAt("c1", baseTime.Add(10*time.Second)),
	}

	var receivedSessions [][]*hits.Hit

	tickerBackend := NewGenericKVTimingWheelBackend("protosessions", storage.NewInMemoryKV())
	backend := NewTestBatchedIOBackend(
		WithGetAllProtosessionsForBucketHandler(
			func(_ *GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse {
				return &GetAllProtosessionsForBucketResponse{
					ProtoSessions: [][]*hits.Hit{unsortedHits},
				}
			}),
	)

	closer := NewTestCloser(WithCloseHandler(func(sessions [][]*hits.Hit) error {
		receivedSessions = sessions
		return nil
	}))

	requeuer := receiver.NewTestStorage(nil)
	settingsRegistry := properties.NewTestSettingRegistry()

	orchestrator := NewOrchestrator(
		ctx,
		backend,
		tickerBackend,
		closer,
		requeuer,
		settingsRegistry,
	)

	// when
	err := orchestrator.processBucket(ctx, 100)

	// then
	assert.NoError(t, err)
	assert.Len(t, receivedSessions, 1)

	sortedHits := receivedSessions[0]
	assert.Len(t, sortedHits, 3)
	assert.True(t, sortedHits[0].MustParsedRequest().ServerReceivedTime.
		Before(sortedHits[1].MustParsedRequest().ServerReceivedTime))
	assert.True(t, sortedHits[1].MustParsedRequest().ServerReceivedTime.
		Before(sortedHits[2].MustParsedRequest().ServerReceivedTime))
}

func TestOrchestrator_ProcessBucket_CleanupBucketMetadata(t *testing.T) {
	// given
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cleanedBuckets []int64

	tickerBackend := NewGenericKVTimingWheelBackend("protosessions", storage.NewInMemoryKV())
	backend := NewTestBatchedIOBackend(
		WithGetAllProtosessionsForBucketHandler(
			func(_ *GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse {
				return &GetAllProtosessionsForBucketResponse{
					ProtoSessions: [][]*hits.Hit{
						{makeTimedHit("c1", 100)},
					},
				}
			}),
		WithRemoveBucketMetadataHandler(
			func(req *RemoveBucketMetadataRequest) *RemoveBucketMetadataResponse {
				cleanedBuckets = append(cleanedBuckets, req.BucketID)
				return &RemoveBucketMetadataResponse{}
			}),
	)

	closer := NewTestCloser()
	requeuer := receiver.NewTestStorage(nil)
	settingsRegistry := properties.NewTestSettingRegistry()

	orchestrator := NewOrchestrator(
		ctx,
		backend,
		tickerBackend,
		closer,
		requeuer,
		settingsRegistry,
	)

	// when
	err := orchestrator.processBucket(ctx, 100)

	// then
	assert.NoError(t, err)
	assert.Len(t, cleanedBuckets, 1)
	// processBucket processes bucket N-1 when passed N
	assert.Equal(t, int64(99), cleanedBuckets[0])
}

func TestOrchestrator_ProcessBucket_SkipsEmptyProtosessions(t *testing.T) {
	// given
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var closedCount int

	tickerBackend := NewGenericKVTimingWheelBackend("protosessions", storage.NewInMemoryKV())
	backend := NewTestBatchedIOBackend(
		WithGetAllProtosessionsForBucketHandler(
			func(_ *GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse {
				return &GetAllProtosessionsForBucketResponse{
					ProtoSessions: [][]*hits.Hit{
						{makeTimedHit("c1", 100)},
						{}, // empty protosession
						{makeTimedHit("c2", 200)},
					},
				}
			}),
	)

	closer := NewTestCloser(WithCloseHandler(func(sessions [][]*hits.Hit) error {
		closedCount = len(sessions)
		return nil
	}))

	requeuer := receiver.NewTestStorage(nil)
	settingsRegistry := properties.NewTestSettingRegistry()

	orchestrator := NewOrchestrator(
		ctx,
		backend,
		tickerBackend,
		closer,
		requeuer,
		settingsRegistry,
	)

	// when
	err := orchestrator.processBucket(ctx, 100)

	// then
	assert.NoError(t, err)
	assert.Equal(t, 2, closedCount) // empty one skipped
}

func TestOrchestrator_ProcessBucket_CleanupErrors(t *testing.T) {
	tests := []struct {
		name           string
		removeHitsErr  error
		removeMetaErr  error
		removeBktErr   error
		expectErrMatch string
	}{
		{
			name:           "remove_hits_error",
			removeHitsErr:  errors.New("redis down"),
			expectErrMatch: "redis down",
		},
		{
			name:           "remove_metadata_error",
			removeMetaErr:  errors.New("metadata cleanup failed"),
			expectErrMatch: "metadata cleanup failed",
		},
		{
			name:           "remove_bucket_error",
			removeBktErr:   errors.New("bucket cleanup failed"),
			expectErrMatch: "bucket cleanup failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// given
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tickerBackend := NewGenericKVTimingWheelBackend("protosessions", storage.NewInMemoryKV())
			backend := NewTestBatchedIOBackend(
				WithGetAllProtosessionsForBucketHandler(
					func(_ *GetAllProtosessionsForBucketRequest) *GetAllProtosessionsForBucketResponse {
						hit := makeTimedHit("c1", 100)
						// Set metadata so the error path can be tested
						SetIsolatedSessionStamp(hit, "test-stamp")
						return &GetAllProtosessionsForBucketResponse{
							ProtoSessions: [][]*hits.Hit{
								{hit},
							},
						}
					}),
				WithRemoveProtoSessionHitsHandler(
					func(_ *RemoveProtoSessionHitsRequest) *RemoveProtoSessionHitsResponse {
						return &RemoveProtoSessionHitsResponse{Err: tc.removeHitsErr}
					}),
				WithRemoveAllHitRelatedMetadataHandler(
					func(_ *RemoveAllHitRelatedMetadataRequest) *RemoveAllHitRelatedMetadataResponse {
						return &RemoveAllHitRelatedMetadataResponse{Err: tc.removeMetaErr}
					}),
				WithRemoveBucketMetadataHandler(
					func(_ *RemoveBucketMetadataRequest) *RemoveBucketMetadataResponse {
						return &RemoveBucketMetadataResponse{Err: tc.removeBktErr}
					}),
			)

			closer := NewTestCloser()
			requeuer := receiver.NewTestStorage(nil)
			settingsRegistry := properties.NewTestSettingRegistry()

			orchestrator := NewOrchestrator(
				ctx,
				backend,
				tickerBackend,
				closer,
				requeuer,
				settingsRegistry,
			)

			// when
			err := orchestrator.processBucket(ctx, 100)

			// then
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectErrMatch)
		})
	}
}

func makeTimedHit(clientID string, offsetSeconds int) *hits.Hit {
	h := hits.New()
	h.ClientID = hits.ClientID(clientID)
	h.AuthoritativeClientID = hits.ClientID(clientID)
	h.PropertyID = "test-property"
	h.Request.ServerReceivedTime = time.Date(2025, 1, 1, 12, 0, offsetSeconds, 0, time.UTC)
	return h
}

func makeTimedHitAt(clientID string, t time.Time) *hits.Hit {
	h := hits.New()
	h.ClientID = hits.ClientID(clientID)
	h.AuthoritativeClientID = hits.ClientID(clientID)
	h.PropertyID = "test-property"
	h.Request.ServerReceivedTime = t
	return h
}
