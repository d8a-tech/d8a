package protosessions

import (
	"errors"
	"fmt"
	"testing"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardingCloser_FallbackToAuthoritativeClientID(t *testing.T) {
	t.Parallel()

	// given: proto-session without isolated client ID metadata (fallback case)
	sharedClientID := hits.ClientID("shared-client-id")
	protosession := []*hits.Hit{
		func() *hits.Hit {
			h := hits.New()
			h.AuthoritativeClientID = sharedClientID
			// No isolated client ID set - should fallback to AuthoritativeClientID
			return h
		}(),
	}

	shardReceived := make([][]*hits.Hit, 0)
	closer := NewShardingCloser(2, func(shardIndex int) Closer {
		return &mockCloser{
			closeFunc: func(protosessions [][]*hits.Hit) error {
				shardReceived = append(shardReceived, protosessions...)
				return nil
			},
		}
	})

	// when
	err := closer.Close([][]*hits.Hit{protosession})
	require.NoError(t, err)

	// then: should still route successfully (using fallback)
	assert.Len(t, shardReceived, 1, "should route proto-session even without isolated client ID")
}

type mockCloser struct {
	closeFunc func([][]*hits.Hit) error
}

func (m *mockCloser) Close(protosessions [][]*hits.Hit) error {
	if m.closeFunc != nil {
		return m.closeFunc(protosessions)
	}
	return nil
}

func TestShardingCloser_AggregatesAllShardErrors(t *testing.T) {
	t.Parallel()

	// given: two shards that each return a distinct error
	errShard0 := fmt.Errorf("shard-0 failed")
	errShard1 := fmt.Errorf("shard-1 failed")

	closer := NewShardingCloser(2, func(shardIndex int) Closer {
		return &mockCloser{
			closeFunc: func(_ [][]*hits.Hit) error {
				if shardIndex == 0 {
					return errShard0
				}
				return errShard1
			},
		}
	})

	// Build proto-sessions that hash to different shards so both children execute.
	protosessions := buildProtosessionsForAllShards(t, 2)

	// when
	err := closer.Close(protosessions)

	// then
	require.Error(t, err)
	assert.True(t, errors.Is(err, errShard0), "should contain shard-0 error")
	assert.True(t, errors.Is(err, errShard1), "should contain shard-1 error")
}

func TestShardingCloser_SingleShardError(t *testing.T) {
	t.Parallel()

	// given: one shard errors, the other succeeds
	errShard0 := fmt.Errorf("shard-0 failed")

	closer := NewShardingCloser(2, func(shardIndex int) Closer {
		return &mockCloser{
			closeFunc: func(_ [][]*hits.Hit) error {
				if shardIndex == 0 {
					return errShard0
				}
				return nil
			},
		}
	})

	protosessions := buildProtosessionsForAllShards(t, 2)

	// when
	err := closer.Close(protosessions)

	// then
	require.Error(t, err)
	assert.True(t, errors.Is(err, errShard0), "should contain shard-0 error")
}

func TestShardingCloser_NoErrors(t *testing.T) {
	t.Parallel()

	// given: all shards succeed
	closer := NewShardingCloser(2, func(_ int) Closer {
		return &mockCloser{
			closeFunc: func(_ [][]*hits.Hit) error {
				return nil
			},
		}
	})

	protosessions := buildProtosessionsForAllShards(t, 2)

	// when
	err := closer.Close(protosessions)

	// then
	require.NoError(t, err)
}

// buildProtosessionsForAllShards creates proto-sessions that are guaranteed to
// hash to each of the n shards by brute-forcing client IDs.
func buildProtosessionsForAllShards(t *testing.T, n int) [][]*hits.Hit {
	t.Helper()

	seen := make(map[int]bool)
	var result [][]*hits.Hit

	for i := 0; len(seen) < n; i++ {
		clientID := hits.ClientID(fmt.Sprintf("client-%d", i))
		h := hits.New()
		h.AuthoritativeClientID = clientID

		sc := &shardingCloser{children: make([]Closer, n)}
		idx := sc.shardFor(GetIsolatedClientID(h))

		if !seen[idx] {
			seen[idx] = true
			result = append(result, []*hits.Hit{h})
		}

		if i > 10000 {
			t.Fatal("could not find client IDs for all shards")
		}
	}

	return result
}
