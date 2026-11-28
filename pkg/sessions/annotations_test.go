package sessions

import (
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sessionsRecorder struct {
	sessions []*schema.Session
}

func (w *sessionsRecorder) Write(sessions ...*schema.Session) error {
	w.sessions = append(w.sessions, sessions...)
	return nil
}

func closerHit(id string, seconds int, annotation bool) *hits.Hit {
	h := hits.New()
	h.ID = id
	h.PropertyID = "property"
	h.Request.ServerReceivedTime = time.Date(2025, 1, 1, 12, 0, seconds, 0, time.UTC)
	if annotation {
		hits.MarkAnnotation(h)
	}
	return h
}

func eventIDs(session *schema.Session) []string {
	ids := make([]string, len(session.Events))
	for i, event := range session.Events {
		ids[i] = event.BoundHit.ID
	}
	return ids
}

func TestDirectCloserTurnsOnlyOrdinaryHitsIntoEvents(t *testing.T) {
	// given
	writer := &sessionsRecorder{}
	var enriched [][]string
	closer := NewDirectCloser(writer, 0, WithAnnotationEnricher(
		func(events []*schema.Event, annotations []*hits.Hit) {
			ids := make([]string, len(annotations))
			for i, a := range annotations {
				ids[i] = a.ID
			}
			enriched = append(enriched, ids)
			for _, event := range events {
				event.Metadata["test.annotations"] = len(annotations)
			}
		},
	))

	// when
	err := closer.Close([][]*hits.Hit{
		{closerHit("o1", 0, false), closerHit("n1", 1, true), closerHit("o2", 2, false)},
		{closerHit("only-note", 3, true)},
	})

	// then
	require.NoError(t, err)
	require.Len(t, writer.sessions, 1, "a proto-session of only annotations writes nothing")
	assert.Equal(t, []string{"o1", "o2"}, eventIDs(writer.sessions[0]))
	assert.Equal(t, [][]string{{"n1"}}, enriched)
	assert.Equal(t, 1, writer.sessions[0].Events[0].Metadata["test.annotations"])
}

func TestDirectCloserIgnoresAnnotationsWithoutAnEnricher(t *testing.T) {
	// given
	writer := &sessionsRecorder{}
	closer := NewDirectCloser(writer, 0)

	// when
	err := closer.Close([][]*hits.Hit{{closerHit("o1", 0, false), closerHit("n1", 1, true)}})

	// then
	require.NoError(t, err)
	require.Len(t, writer.sessions, 1)
	assert.Equal(t, []string{"o1"}, eventIDs(writer.sessions[0]))
	assert.Empty(t, writer.sessions[0].Events[0].Metadata)
}
