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

func newTestWriter(ctx context.Context, mockDriver warehouse.Driver) SessionWriter {
	return NewSessionWriter(
		ctx,
		warehouse.NewStaticDriverRegistry(mockDriver),
		schema.NewStaticColumnsRegistry(
			map[string]schema.Columns{},
			schema.NewColumns(
				[]schema.SessionColumn{},
				[]schema.EventColumn{},
				[]schema.SessionScopedEventColumn{},
			),
		),
		schema.NewStaticLayoutRegistry(
			map[string]schema.Layout{},
			schema.NewEmbeddedSessionColumnsLayout(
				"events",
				"session_",
			),
		),
		splitter.NewStaticRegistry(splitter.NewNoop()),
		WithConcurrency(5),
	)
}

func testSession(propertyID, sessionID, eventID string) *schema.Session {
	return &schema.Session{
		PropertyID: propertyID,
		Values: map[string]any{
			"session_id":        sessionID,
			"session_timestamp": time.Now(),
		},
		Events: []*schema.Event{
			{
				BoundHit: &hits.Hit{ID: eventID},
				Values: map[string]any{
					"id":            eventID,
					"name":          "test",
					"timestamp_utc": time.Now(),
				},
			},
		},
	}
}

func TestWriter(t *testing.T) {
	// given
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	mockDriver := warehouse.NewMockWarehouseDriver()
	writer := newTestWriter(ctx, mockDriver)
	sessions := []*schema.Session{
		testSession("1", "1", "1"),
		testSession("1", "2", "2"),
	}

	// when
	err := writer.Write(sessions...)

	// then
	assert.NoError(t, err)
	assert.Len(t, mockDriver.WriteCalls, 1) // Should be a single write, batching should kick in
}

func TestWriter_SkipsEmptyRows(t *testing.T) {
	// given
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	mockDriver := warehouse.NewMockWarehouseDriver()
	writer := newTestWriter(ctx, mockDriver)

	// A session with all events filtered out (all broken) produces zero rows.
	// The brokenFilteringLayout drops sessions whose entire event list is broken,
	// resulting in len(Rows)==0 for every table — the writer must skip the write.
	brokenSession := &schema.Session{
		PropertyID: "1",
		Values: map[string]any{
			"session_id":        "broken-1",
			"session_timestamp": time.Now(),
		},
		Events: []*schema.Event{
			{
				BoundHit: &hits.Hit{ID: "broken-event-1"},
				Values:   map[string]any{},
				IsBroken: true,
			},
		},
	}

	// when
	err := writer.Write(brokenSession)

	// then
	assert.NoError(t, err)
	assert.Empty(t, mockDriver.WriteCalls,
		"sessions with all broken events produce empty rows and must not trigger warehouse writes")
}
