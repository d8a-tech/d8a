package bolt

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/d8a-tech/d8a/pkg/encoding"
	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protosessions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

const sessionID = hits.ClientID("session-1")

type annotationFixture struct {
	t       *testing.T
	path    string
	db      *bolt.DB
	backend *boltBatchedIOBackend
}

func newAnnotationFixture(t *testing.T) *annotationFixture {
	f := &annotationFixture{t: t, path: filepath.Join(t.TempDir(), "bolt.db")}
	f.open()
	t.Cleanup(func() { _ = f.db.Close() })
	return f
}

func (f *annotationFixture) open() {
	db, err := bolt.Open(f.path, 0o600, nil)
	require.NoError(f.t, err)
	backend, err := NewBatchedProtosessionsIOBackend(db, encoding.CBOREncoder, encoding.CBORDecoder)
	require.NoError(f.t, err)
	f.db, f.backend = db, backend.(*boltBatchedIOBackend)
}

func (f *annotationFixture) reopen() {
	require.NoError(f.t, f.db.Close())
	f.open()
}

// openSession stores one ordinary hit for the session, closing in the given bucket.
func (f *annotationFixture) openSession(id hits.ClientID, closingBucket int64) {
	hit := hits.New()
	hit.ID = "ordinary-" + string(id)
	_, _, marks := f.backend.HandleBatch(context.Background(),
		[]*protosessions.AppendHitsToProtoSessionRequest{protosessions.NewAppendHitsToProtoSessionRequest(id, []*hits.Hit{hit})},
		nil,
		[]*protosessions.MarkProtoSessionClosingForGivenBucketRequest{
			protosessions.NewMarkProtoSessionClosingForGivenBucketRequest(id, closingBucket),
		},
	)
	require.NoError(f.t, marks[0].Err)
}

func (f *annotationFixture) attach(id hits.ClientID, nowBucket int64, eventBuckets []int64, annotations ...*hits.Hit) []bool {
	responses := f.backend.AttachAnnotations(context.Background(), []*protosessions.AttachAnnotationsRequest{{
		ProtoSessionID: id, Annotations: annotations, EventBuckets: eventBuckets, NowBucket: nowBucket,
	}})
	require.NoError(f.t, responses[0].Err)
	return responses[0].Attached
}

func (f *annotationFixture) claim(id hits.ClientID) []*hits.Hit {
	responses := f.backend.ClaimForClose(context.Background(), []*protosessions.ClaimForCloseRequest{{ProtoSessionID: id}})
	require.NoError(f.t, responses[0].Err)
	return responses[0].Annotations
}

func annotation(id string) *hits.Hit {
	hit := hits.New()
	hit.ID = id
	hits.MarkAnnotation(hit)
	return hit
}

func ids(hitList []*hits.Hit) []string {
	out := make([]string, 0, len(hitList))
	for _, hit := range hitList {
		out = append(out, hit.ID)
	}
	return out
}

func TestAttachAnnotationsToOpenSession(t *testing.T) {
	// given
	f := newAnnotationFixture(t)
	f.openSession(sessionID, 10)

	// when
	attached := f.attach(sessionID, 5, []int64{6, 7}, annotation("a1"), annotation("a2"))

	// then
	assert.Equal(t, []bool{true, true}, attached)
	assert.ElementsMatch(t, []string{"a1", "a2"}, ids(f.claim(sessionID)))
}

func TestAttachAnnotationsRefusesSessionsThatAreNotOpen(t *testing.T) {
	testCases := []struct {
		name      string
		prepare   func(f *annotationFixture)
		session   hits.ClientID
		nowBucket int64
	}{
		{name: "no such session", prepare: func(*annotationFixture) {}, session: "missing", nowBucket: 5},
		{
			name:      "logically expired but not cleaned up",
			prepare:   func(f *annotationFixture) { f.openSession(sessionID, 10) },
			session:   sessionID,
			nowBucket: 10,
		},
		{
			name: "claimed for close",
			prepare: func(f *annotationFixture) {
				f.openSession(sessionID, 10)
				f.claim(sessionID)
			},
			session:   sessionID,
			nowBucket: 5,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			f := newAnnotationFixture(t)
			tc.prepare(f)

			// when
			attached := f.attach(tc.session, tc.nowBucket, []int64{tc.nowBucket}, annotation("a1"))

			// then
			assert.Equal(t, []bool{false}, attached)
			assert.Empty(t, f.claim(tc.session))
		})
	}
}

func TestAttachAnnotationsRefusesAnnotationsAfterTheSessionDeadline(t *testing.T) {
	// given: the session closes in bucket 10; one annotation happened in bucket 10
	f := newAnnotationFixture(t)
	f.openSession(sessionID, 10)

	// when
	attached := f.attach(sessionID, 5, []int64{9, 10}, annotation("in-time"), annotation("late"))

	// then
	assert.Equal(t, []bool{true, false}, attached)
	assert.Equal(t, []string{"in-time"}, ids(f.claim(sessionID)))
}

func TestAttachAnnotationsDoesNotMoveTheSessionDeadline(t *testing.T) {
	// given
	f := newAnnotationFixture(t)
	f.openSession(sessionID, 10)

	// when
	f.attach(sessionID, 5, []int64{9}, annotation("a1"))

	// then
	responses := f.backend.GetAllProtosessionsForBucket(context.Background(),
		[]*protosessions.GetAllProtosessionsForBucketRequest{protosessions.NewGetAllProtosessionsForBucketRequest(10)})
	require.NoError(t, responses[0].Err)
	require.Len(t, responses[0].ProtoSessions, 1)
	assert.Equal(t, []string{"ordinary-session-1"}, ids(responses[0].ProtoSessions[0]))
}

func TestAttachAnnotationsIsIdempotentAndBounded(t *testing.T) {
	// given
	f := newAnnotationFixture(t)
	f.openSession(sessionID, 10)
	first := annotation("a0")
	f.attach(sessionID, 5, []int64{6}, first)

	// when: the same annotation again (a retried task), then more than the limit
	again := f.attach(sessionID, 5, []int64{6}, first)
	many := make([]*hits.Hit, protosessions.MaxAnnotationsPerProtoSession)
	buckets := make([]int64, len(many))
	for i := range many {
		many[i] = annotation(fmt.Sprintf("b%d", i))
		buckets[i] = 6
	}
	attached := f.attach(sessionID, 5, buckets, many...)

	// then
	assert.Equal(t, []bool{true}, again)
	accepted := 0
	for _, ok := range attached {
		if ok {
			accepted++
		}
	}
	assert.Equal(t, protosessions.MaxAnnotationsPerProtoSession-1, accepted)
	assert.Len(t, f.claim(sessionID), protosessions.MaxAnnotationsPerProtoSession)
}

func TestAttachAnnotationsRespectsTheByteLimit(t *testing.T) {
	// given
	f := newAnnotationFixture(t)
	f.openSession(sessionID, 10)
	big := annotation("big")
	big.EventName = strings.Repeat("x", protosessions.MaxAnnotationBytesPerProtoSession)

	// when
	attached := f.attach(sessionID, 5, []int64{6, 6}, big, annotation("small"))

	// then
	assert.Equal(t, []bool{false, true}, attached)
}

func TestClaimForCloseIsIdempotentAndSurvivesRestart(t *testing.T) {
	// given
	f := newAnnotationFixture(t)
	f.openSession(sessionID, 10)
	f.attach(sessionID, 5, []int64{6}, annotation("a1"))
	first := f.claim(sessionID)

	// when
	f.reopen()
	second := f.claim(sessionID)
	attachedAfterClaim := f.attach(sessionID, 5, []int64{6}, annotation("a2"))

	// then
	assert.Equal(t, ids(first), ids(second))
	assert.Equal(t, []bool{false}, attachedAfterClaim)
}

func TestRemovingASessionRemovesItsAnnotationsAndClaim(t *testing.T) {
	// given
	f := newAnnotationFixture(t)
	f.openSession(sessionID, 10)
	f.attach(sessionID, 5, []int64{6}, annotation("a1"))
	f.claim(sessionID)

	// when
	removed, _, _ := f.backend.Cleanup(context.Background(),
		[]*protosessions.RemoveProtoSessionHitsRequest{protosessions.NewRemoveProtoSessionHitsRequest(sessionID)}, nil, nil)
	require.NoError(t, removed[0].Err)
	f.openSession(sessionID, 20)

	// then: the reopened session starts without annotations or a claim
	assert.Equal(t, []bool{true}, f.attach(sessionID, 15, []int64{16}, annotation("a2")))
	assert.Equal(t, []string{"a2"}, ids(f.claim(sessionID)))
}

func TestLookupIdentifierOwnersIsReadOnly(t *testing.T) {
	// given: one identifier registered by an ordinary hit
	f := newAnnotationFixture(t)
	owner := hits.New()
	owner.AuthoritativeClientID = "owner"
	stamp := func(*hits.Hit) string { return "stamp-1" }
	other := func(*hits.Hit) string { return "stamp-2" }
	f.backend.GetIdentifierConflicts(context.Background(), []*protosessions.IdentifierConflictRequest{
		protosessions.NewIdentifierConflictRequest(owner, "session_stamp", stamp),
	})
	visitor := annotation("a1")
	visitor.AuthoritativeClientID = "visitor"

	// when
	found := f.backend.LookupIdentifierOwners(context.Background(), []*protosessions.IdentifierConflictRequest{
		protosessions.NewIdentifierConflictRequest(visitor, "session_stamp", stamp),
		protosessions.NewIdentifierConflictRequest(visitor, "session_stamp", other),
	})

	// then
	assert.Equal(t, &protosessions.LookupIdentifierOwnerResponse{Owner: "owner", Found: true}, found[0])
	assert.Equal(t, &protosessions.LookupIdentifierOwnerResponse{}, found[1])
	// Had the lookup registered stamp-2 for the visitor, another client would now conflict.
	third := hits.New()
	third.AuthoritativeClientID = "third"
	conflicts := f.backend.GetIdentifierConflicts(context.Background(), []*protosessions.IdentifierConflictRequest{
		protosessions.NewIdentifierConflictRequest(third, "session_stamp", other),
	})
	assert.False(t, conflicts[0].HasConflict, "the lookup must not have registered stamp-2")
}
