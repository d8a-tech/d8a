package protosessions

import (
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

func TestShardingCloser_ShardErrors(t *testing.T) {
	t.Parallel()

	errShard0 := fmt.Errorf("shard-0 failed")
	errShard1 := fmt.Errorf("shard-1 failed")

	cases := []struct {
		name        string
		shardErrors map[int]error // shard index → error it returns
		wantErr     bool
		wantWrapped []error // errors that must be present via errors.Is
	}{
		{
			name:        "all shards error",
			shardErrors: map[int]error{0: errShard0, 1: errShard1},
			wantErr:     true,
			wantWrapped: []error{errShard0, errShard1},
		},
		{
			name:        "single shard error",
			shardErrors: map[int]error{0: errShard0},
			wantErr:     true,
			wantWrapped: []error{errShard0},
		},
		{
			name:        "no errors",
			shardErrors: map[int]error{},
			wantErr:     false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// given
			closer := NewShardingCloser(2, func(shardIndex int) Closer {
				return &mockCloser{
					closeFunc: func(_ [][]*hits.Hit) error {
						return tc.shardErrors[shardIndex]
					},
				}
			})

			protosessions := buildProtosessionsForAllShards(t, 2)

			// when
			err := closer.Close(protosessions)

			// then
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			for _, wrapped := range tc.wantWrapped {
				assert.ErrorIs(t, err, wrapped)
			}
		})
	}
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
