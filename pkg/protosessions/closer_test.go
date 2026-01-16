package protosessions

import (
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
