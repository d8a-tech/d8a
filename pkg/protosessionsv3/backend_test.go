package protosessionsv3

import (
	"context"
	"testing"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNaiveGenericStorageBatchedIOBackend_GetIdentifierConflicts(t *testing.T) {
	// given
	kv := storage.NewInMemoryKV()
	set := storage.NewInMemorySet()
	backend := NewNaiveGenericStorageBatchedIOBackend(kv, set, encoding.JSONEncoder, encoding.JSONDecoder)

	// when
	results := backend.GetIdentifierConflicts(context.Background(), []*IdentifierConflictRequest{
		{
			IdentifierType: "session_stamp",
			Hit: &hits.Hit{
				AuthoritativeClientID: "client1",
				IP:                    "192.168.1.1",
			},
			ExtractIdentifier: func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		},
		{
			IdentifierType: "session_stamp",
			Hit: &hits.Hit{
				AuthoritativeClientID: "client1",
				IP:                    "192.168.1.1",
			},
			ExtractIdentifier: func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		},
		{
			IdentifierType: "session_stamp",
			Hit: &hits.Hit{
				AuthoritativeClientID: "client2",
				IP:                    "192.168.1.1",
			},
			ExtractIdentifier: func(h *hits.Hit) string {
				return h.SessionStamp()
			},
		},
	})

	// then
	require.Len(t, results, 3)
	require.NoError(t, results[0].Err)
	require.False(t, results[0].HasConflict)
	require.Equal(t, hits.ClientID(""), results[0].ConflictsWith)
	require.NoError(t, results[1].Err)
	require.False(t, results[1].HasConflict)
	require.Equal(t, hits.ClientID(""), results[1].ConflictsWith)
	require.NoError(t, results[2].Err)
	require.True(t, results[2].HasConflict)
	require.Equal(t, hits.ClientID("client1"), results[2].ConflictsWith)
}
