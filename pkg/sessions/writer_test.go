package sessions

import (
	"context"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/splitter"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	// given
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	mockDriver := warehouse.NewMockDriver()
	writer := &sessionWriterImpl{
		writeTimeout: 30 * time.Second,
		concurrency:  5,
		warehouseRegistry: warehouse.NewStaticDriverRegistry(
			mockDriver,
		),
		warehouseCache: createDefaultCache[warehouse.Driver](),
		columnsRegistry: schema.NewStaticColumnsRegistry(
			map[string]schema.Columns{},
			schema.NewColumns(
				[]schema.SessionColumn{},
				[]schema.EventColumn{},
				[]schema.SessionScopedEventColumn{},
			),
		),
		columnsCache: createDefaultCache[schema.Columns](),
		layoutRegistry: schema.NewStaticLayoutRegistry(
			map[string]schema.Layout{},
			schema.NewEmbeddedSessionColumnsLayout(
				"events",
				"session_",
			),
		),
		splitterRegistry: splitter.NewStaticSplitterRegistry(
			splitter.NewNoop(),
		),
		layoutsCache: createDefaultCache[schema.Layout](),
		cacheTTL:     5 * time.Minute,
		parentCtx:    ctx,
	}
	sessions := []*schema.Session{
		{
			PropertyID: "1",
			Values: map[string]any{
				"session_id":        "1",
				"session_timestamp": time.Now(),
			},
			Events: []*schema.Event{
				{
					BoundHit: &hits.Hit{
						ID: "1",
					},
					Values: map[string]any{
						"id":            "1",
						"name":          "test",
						"timestamp_utc": time.Now(),
					},
				},
			},
		},
		{
			PropertyID: "1",
			Values: map[string]any{
				"session_id":        "2",
				"session_timestamp": time.Now(),
			},
			Events: []*schema.Event{
				{
					BoundHit: &hits.Hit{
						ID: "2",
					},
					Values: map[string]any{
						"id":            "2",
						"name":          "test",
						"timestamp_utc": time.Now(),
					},
				},
			},
		},
	}

	// when
	err := writer.Write(sessions...)

	// then
	assert.NoError(t, err)
	assert.Len(t, mockDriver.Writes, 1) // Should be a single write, batching should kick in
}
