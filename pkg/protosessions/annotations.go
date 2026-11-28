package protosessions

import (
	"context"

	"github.com/d8a-tech/d8a/pkg/hits"
)

// Annotation limits per proto-session: annotations never displace ordinary hits.
const (
	MaxAnnotationsPerProtoSession     = 64
	MaxAnnotationBytesPerProtoSession = 512 * 1024
)

// AnnotationBackend stores annotation hits (see hits.RoleAnnotation) next to the proto-session
// they enrich. A BatchedIOBackend that doesn't implement it gets no annotations: the
// orchestrator drops them.
//
// Annotations never create a proto-session, move its closing bucket or register identifiers.
// Attaching and claiming for close are atomic with respect to each other, so an annotation is
// either part of the closed session or reported as not attached.
type AnnotationBackend interface {
	// LookupIdentifierOwners returns the client that currently owns each identifier, without
	// registering anything (unlike GetIdentifierConflicts).
	LookupIdentifierOwners(
		ctx context.Context,
		requests []*IdentifierConflictRequest,
	) []*LookupIdentifierOwnerResponse
	// AttachAnnotations stores annotations on open proto-sessions.
	AttachAnnotations(ctx context.Context, requests []*AttachAnnotationsRequest) []*AttachAnnotationsResponse
	// ClaimForClose marks proto-sessions as being closed, so no annotation attaches anymore,
	// and returns the annotations attached so far. Repeated claims return the same annotations.
	ClaimForClose(ctx context.Context, requests []*ClaimForCloseRequest) []*ClaimForCloseResponse
}

// LookupIdentifierOwnerResponse is the current owner of an identifier, if any.
type LookupIdentifierOwnerResponse struct {
	Owner hits.ClientID
	Found bool
	Err   error
}

// AttachAnnotationsRequest attaches annotations to a proto-session if it is still open: it
// exists, is not claimed for close, and its closing bucket is after NowBucket (the timing
// wheel's current bucket). Each annotation is attached only if the closing bucket is also after
// its own event bucket (EventBuckets[i], the bucket of its time): an annotation can't belong to a
// session whose inactivity deadline passed before it happened.
type AttachAnnotationsRequest struct {
	ProtoSessionID hits.ClientID
	Annotations    []*hits.Hit
	EventBuckets   []int64
	NowBucket      int64
}

// AttachAnnotationsResponse reports, per annotation, whether it was attached.
type AttachAnnotationsResponse struct {
	Attached []bool
	Err      error
}

// ClaimForCloseRequest claims one proto-session for close.
type ClaimForCloseRequest struct {
	ProtoSessionID hits.ClientID
}

// ClaimForCloseResponse carries the annotations of a claimed proto-session.
type ClaimForCloseResponse struct {
	Annotations []*hits.Hit
	Err         error
}
