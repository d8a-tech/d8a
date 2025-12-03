package protosessionsv3

import (
	"context"
	"testing"

	"github.com/d8a-tech/d8a/pkg/columns/columntests"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestWorker(t *testing.T) {
	results := make(map[hits.ClientID][]*hits.Hit)
	clashes := make(map[string]string)
	evictions := make([]*hits.Hit, 0)
	tickerStateBackend := NewGenericKVTimingWheelBackend("protosessions", storage.NewInMemoryKV())
	backend := NewTestBatchedIOBackend(
		WithAppendHitsHandler(func(request *AppendHitsToProtoSessionRequest) *AppendHitsToProtoSessionResponse {
			if results[request.ProtoSessionID] == nil {
				results[request.ProtoSessionID] = make([]*hits.Hit, 0)
			}
			results[request.ProtoSessionID] = append(results[request.ProtoSessionID], request.Hits...)
			return &AppendHitsToProtoSessionResponse{
				Err: nil,
			}
		}),
		WithIdentifierConflictHandler(func(request *IdentifierConflictRequest) *IdentifierConflictResponse {
			toCompare := request.ExtractIdentifier(request.Hit)
			existing, ok := clashes[toCompare]
			if ok {
				return &IdentifierConflictResponse{
					Err:           nil,
					HasConflict:   true,
					ConflictsWith: hits.ClientID(existing),
				}
			}
			clashes[toCompare] = string(request.Hit.AuthoritativeClientID)
			return &IdentifierConflictResponse{
				Err:           nil,
				HasConflict:   false,
				ConflictsWith: "",
			}
		}),
	)
	closer := NewTestCloser()
	requeuer := receiver.NewTestStorage(func(hits []*hits.Hit) error {
		evictions = append(evictions, hits...)
		return nil
	})
	settingsRegistry := properties.NewTestSettingRegistry()
	handler := Handler(
		context.Background(),
		backend,
		tickerStateBackend,
		closer,
		requeuer,
		settingsRegistry,
		EvictWholeProtosessionStrategy,
	)
	assert.Nil(t, handler(map[string]string{}, &hits.HitProcessingTask{
		Hits: []*hits.Hit{
			columntests.TestHitOne(),
			columntests.TestHitTwo(),
			columntests.TestHitThree(),
			columntests.TestHitFour(),
		},
	}))
	assert.Len(t, results, 3)
	assert.Len(t, results[columntests.TestHitOne().AuthoritativeClientID], 1)
	assert.Len(t, results[columntests.TestHitTwo().AuthoritativeClientID], 1)
	assert.Len(t, results[columntests.TestHitFour().AuthoritativeClientID], 1)
	assert.Equal(t, evictions[0].ID, columntests.TestHitThree().ID)
	assert.Len(t, evictions, 1)
}
