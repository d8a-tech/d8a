// Package e2e provides end-to-end testing functionality for the tracker-api
package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFullE2EWroteToWarehouse(t *testing.T) {
	withFullRunningServer(t, func(runningServer runningServer) {
		striker := NewGA4RequestGenerator("localhost", runningServer.port)

		if err := striker.Replay([]HitSequenceItem{
			{
				ClientID:     "client-1",
				EventType:    "page_view",
				SessionStamp: "127.0.0.1",
				Description:  "client 1",
				SleepBefore:  0,
			},
			{
				ClientID:     "client-2",
				EventType:    "scroll",
				SessionStamp: "127.0.0.2",
				Description:  "client 2",
				SleepBefore:  time.Millisecond * 100,
			},
			{
				ClientID:     "client-3",
				EventType:    "page_view",
				SessionStamp: "127.0.0.1",
				Description:  "client 3 (should be same session as client 1)",
				SleepBefore:  time.Millisecond * 100,
			},
		}); err != nil {
			t.Fatalf("Failed to replay GA4 sequence: %v", err)
		}

		require.True(
			t,
			runningServer.logs.waitFor("flushing batch of size 3", 10*time.Second),
			"all three events should be flushed together",
		)
	})
}
