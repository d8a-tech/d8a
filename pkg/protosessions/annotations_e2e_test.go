package protosessions_test

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/d8a-tech/d8a/pkg/bolt"
	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/pings"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protosessions"
	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bbolt "go.etcd.io/bbolt"
)

var baseTime = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

// pipeline is the worker side of the tracker as wired in production: the orchestrator over the
// deduplicating Bolt backend, default identifier isolation, 30 s session timeout and session
// stamp joins. The closer records what it receives; closeHook can block it.
type pipeline struct {
	t         *testing.T
	db        *bbolt.DB
	handler   func(map[string]string, *hits.HitProcessingTask) *worker.Error
	mu        sync.Mutex
	closed    [][]*hits.Hit
	closeHook func()
}

func newPipeline(t *testing.T, settingsOpts ...properties.TestSettingsOption) *pipeline {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "bolt.db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	boltBackend, err := bolt.NewBatchedProtosessionsIOBackend(db, encoding.CBOREncoder, encoding.CBORDecoder)
	require.NoError(t, err)

	p := &pipeline{t: t, db: db}
	closer := protosessions.NewTestCloser(protosessions.WithCloseHandler(func(batch [][]*hits.Hit) error {
		if p.closeHook != nil {
			p.closeHook()
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.closed = append(p.closed, batch...)
		return nil
	}))
	p.handler = protosessions.Handler(
		ctx,
		protosessions.NewDeduplicatingBatchedIOBackend(boltBackend),
		protosessions.NewGenericKVTimingWheelBackend("protosessions", storage.NewInMemoryKV()),
		closer,
		receiver.NewTestStorage(func([]*hits.Hit) error { return nil }),
		properties.NewTestSettingRegistry(settingsOpts...),
	)
	return p
}

func (p *pipeline) send(batch ...*hits.Hit) {
	require.Nil(p.t, p.handler(map[string]string{}, &hits.HitProcessingTask{Hits: batch}))
}

// ping advances the worker's time, as the receiver's pings do.
func (p *pipeline) ping(seconds int) {
	require.Nil(p.t, p.handler(map[string]string{
		pings.IsPingMetadataKey:        pings.IsPingMetadataValue,
		pings.PingTimestampMetadataKey: baseTime.Add(time.Duration(seconds) * time.Second).Format(time.RFC3339),
	}, &hits.HitProcessingTask{Hits: []*hits.Hit{}}))
}

// closeUpTo advances the worker's time and waits until n proto-sessions were closed.
func (p *pipeline) closeUpTo(seconds, n int) [][]*hits.Hit {
	p.ping(seconds)
	require.Eventually(p.t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.closed) >= n
	}, 10*time.Second, 5*time.Millisecond)
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]*hits.Hit(nil), p.closed...)
}

func (p *pipeline) countKeys(bucket string) int {
	count := 0
	require.NoError(p.t, p.db.View(func(tx *bbolt.Tx) error {
		if b := tx.Bucket([]byte(bucket)); b != nil {
			return b.ForEach(func(_, _ []byte) error { count++; return nil })
		}
		return nil
	}))
	return count
}

func hitAt(clientID, id string, seconds int) *hits.Hit {
	h := hits.New()
	h.ID = id
	h.ClientID = hits.ClientID(clientID)
	h.AuthoritativeClientID = hits.ClientID(clientID)
	h.PropertyID = "test-property"
	h.Request.QueryParams.Set("fss", "stamp-"+clientID)
	h.Request.ServerReceivedTime = baseTime.Add(time.Duration(seconds) * time.Second)
	return h
}

func annotationAt(clientID, id string, seconds int) *hits.Hit {
	h := hitAt(clientID, id, seconds)
	hits.MarkAnnotation(h)
	return h
}

// summary describes closed proto-sessions as sorted "ordinary IDs | annotation IDs" lines.
func summary(closed [][]*hits.Hit) []string {
	lines := make([]string, 0, len(closed))
	for _, session := range closed {
		var ordinary, annotations []string
		for _, h := range session {
			if hits.IsAnnotation(h) {
				annotations = append(annotations, h.ID)
			} else {
				ordinary = append(ordinary, h.ID)
			}
		}
		sort.Strings(ordinary)
		sort.Strings(annotations)
		lines = append(lines, joinIDs(ordinary)+" | "+joinIDs(annotations))
	}
	sort.Strings(lines)
	return lines
}

func joinIDs(ids []string) string {
	return strings.Join(ids, ",")
}

// ordinaryOnly keeps the ordinary part of summary lines.
func ordinaryOnly(lines []string) []string {
	out := make([]string, len(lines))
	for i, line := range lines {
		out[i], _, _ = strings.Cut(line, " | ")
	}
	return out
}

func TestAnnotationsLeaveOrdinarySessionsUnchanged(t *testing.T) {
	// given: the same ordinary hits, alone and interleaved with annotations (one of them for a
	// client with no ordinary activity)
	ordinary := func() []*hits.Hit {
		return []*hits.Hit{hitAt("a", "a1", 0), hitAt("b", "b1", 2), hitAt("a", "a2", 5)}
	}
	alone := newPipeline(t)
	annotated := newPipeline(t)

	// when
	alone.send(ordinary()...)
	annotated.send(append(ordinary(),
		annotationAt("a", "a-note", 3), annotationAt("b", "b-note", 4), annotationAt("c", "c-note", 6))...)
	withoutAnnotations := summary(alone.closeUpTo(200, 2))
	withAnnotations := summary(annotated.closeUpTo(200, 2))

	// then
	assert.Equal(t, []string{"a1,a2 | ", "b1 | "}, withoutAnnotations)
	assert.Equal(t, ordinaryOnly(withoutAnnotations), ordinaryOnly(withAnnotations))
	assert.Equal(t, []string{"a1,a2 | a-note", "b1 | b-note"}, withAnnotations)
}

func TestAnAnnotationDoesNotBridgeTheInactivityTimeout(t *testing.T) {
	// given: ordinary hits at 0 s and 35 s with a 30 s timeout; an annotation at 20 s, one at
	// 31 s (after the first session's deadline, before it is closed)
	run := func(withAnnotations bool) []string {
		p := newPipeline(t)
		p.send(hitAt("a", "first", 0))
		if withAnnotations {
			p.send(annotationAt("a", "in-session", 20))
			p.send(annotationAt("a", "after-deadline", 31))
		}
		p.closeUpTo(33, 1)
		p.send(hitAt("a", "second", 35))
		return summary(p.closeUpTo(200, 2))
	}

	// when
	without := run(false)
	with := run(true)

	// then: two sessions either way; only the in-session annotation is attached
	assert.Equal(t, []string{"first | ", "second | "}, without)
	assert.Equal(t, []string{"first | in-session", "second | "}, with)
}

func TestAnnotationsWithoutAnOpenSessionLeaveNothingBehind(t *testing.T) {
	// given
	p := newPipeline(t)
	identifiersBefore := p.countKeys("identifiers")

	// when: annotation-only traffic, then an ordinary session for another client
	p.send(annotationAt("ghost", "ghost-note", 0))
	p.send(hitAt("a", "a1", 1))
	closed := p.closeUpTo(200, 1)

	// then
	assert.Equal(t, []string{"a1 | "}, summary(closed))
	// Cleanup runs right after the close; wait for it rather than racing it.
	require.Eventually(t, func() bool {
		return p.countKeys("annotations") == 0 && p.countKeys("closeClaims") == 0 &&
			p.countKeys("identifiers") == identifiersBefore
	}, 5*time.Second, 5*time.Millisecond, "annotation state and identifiers are gone after the close")
}

func TestAnAnnotationNeverRegistersIdentifiersOrCausesEviction(t *testing.T) {
	// given: a session for client a; an annotation from client b sharing a's session stamp would
	// have been evicted into a's session had it been an ordinary hit
	p := newPipeline(t)
	p.send(hitAt("a", "a1", 0))
	idsBefore := p.countKeys("identifiers")
	note := annotationAt("b", "b-note", 1)
	note.Request.QueryParams.Set("fss", "stamp-a")
	stranger := annotationAt("z", "z-note", 2)
	stranger.Request.QueryParams.Set("fss", "stamp-unknown")

	// when
	p.send(note, stranger)

	// then: the shared stamp resolves read-only to a's session; nothing new is registered
	assert.Equal(t, idsBefore, p.countKeys("identifiers"))
	assert.Equal(t, []string{"a1 | b-note"}, summary(p.closeUpTo(200, 1)))
}

func TestAnAnnotationDoesNotCrossProperties(t *testing.T) {
	// given: the same client ID on another property
	p := newPipeline(t)
	p.send(hitAt("a", "a1", 0))
	other := annotationAt("a", "other-property-note", 1)
	other.PropertyID = "other-property"

	// when
	p.send(other)

	// then
	assert.Equal(t, []string{"a1 | "}, summary(p.closeUpTo(200, 1)))
}

func TestAnAnnotationArrivingWhileItsSessionIsBeingClosedIsNotAttached(t *testing.T) {
	// given: the closer blocks while closing a's session (already claimed)
	p := newPipeline(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	p.closeHook = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	p.send(hitAt("a", "a1", 0), annotationAt("a", "early-note", 1))
	p.ping(200)
	<-entered

	// when: an annotation for the session arrives during the close
	p.send(annotationAt("a", "late-note", 2))
	close(release)

	// then: the close has exactly what was attached before the claim; nothing is stranded
	closed := p.closeUpTo(200, 1)
	assert.Equal(t, []string{"a1 | early-note"}, summary(closed))
	require.Eventually(t, func() bool { return p.countKeys("annotations") == 0 }, 5*time.Second, 5*time.Millisecond)
}
