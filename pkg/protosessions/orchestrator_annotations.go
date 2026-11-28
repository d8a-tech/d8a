package protosessions

import (
	"context"
	"errors"
	"sort"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	annotationsAttachedCounter metric.Int64Counter
	annotationsDroppedCounter  metric.Int64Counter
)

func init() {
	meter := otel.GetMeterProvider().Meter("protosessions")
	annotationsAttachedCounter, _ = meter.Int64Counter(
		"protosessions.annotations.attached",
		metric.WithDescription("Annotation hits attached to an open proto-session"),
	)
	annotationsDroppedCounter, _ = meter.Int64Counter(
		"protosessions.annotations.dropped",
		metric.WithDescription("Annotation hits dropped: no open proto-session, ambiguous owner or limits"),
	)
}

// partitionAnnotations splits a batch into ordinary hits (original order) and annotations,
// sorted deterministically by time, then ID.
func partitionAnnotations(batch []*hits.Hit) (ordinary, annotations []*hits.Hit) {
	for _, hit := range batch {
		if hits.IsAnnotation(hit) {
			annotations = append(annotations, hit)
		} else {
			ordinary = append(ordinary, hit)
		}
	}
	sort.SliceStable(annotations, func(i, j int) bool {
		ti := annotations[i].MustParsedRequest().ServerReceivedTime
		tj := annotations[j].MustParsedRequest().ServerReceivedTime
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return annotations[i].ID < annotations[j].ID
	})
	return ordinary, annotations
}

// attachAnnotations attaches annotations to the open proto-sessions they belong to. They never
// go through the ordinary path: no conflict registration, eviction, outdated-hit cleanup,
// closing bucket mark or timing wheel update, so they can't create, extend or merge sessions.
func (o *Orchestrator) attachAnnotations(ctx context.Context, annotations []*hits.Hit) ProtosessionError {
	if len(annotations) == 0 {
		return nil
	}
	backend, ok := o.backend.(AnnotationBackend)
	if !ok {
		logrus.Debugf("dropping %d annotations: the backend doesn't support them", len(annotations))
		annotationsDroppedCounter.Add(ctx, int64(len(annotations)))
		return nil
	}

	resolved, dropped, err := o.resolveAnnotationOwners(ctx, backend, annotations)
	if err != nil {
		return err
	}
	requests := o.buildAttachRequests(resolved)
	attachedCount := int64(0)
	for _, response := range backend.AttachAnnotations(ctx, requests) {
		if response.Err != nil {
			return NewErrorCausingTaskRetry(response.Err)
		}
		for _, attached := range response.Attached {
			if attached {
				attachedCount++
			} else {
				dropped++
			}
		}
	}
	annotationsAttachedCounter.Add(ctx, attachedCount)
	annotationsDroppedCounter.Add(ctx, dropped)
	return nil
}

// resolveAnnotationOwners finds, read-only, the proto-session each annotation belongs to, with
// the same isolation as ordinary hits. When session stamps or user IDs join sessions, a
// registered owner of the annotation's identifiers wins; owners that disagree make it ambiguous.
func (o *Orchestrator) resolveAnnotationOwners(
	ctx context.Context,
	backend AnnotationBackend,
	annotations []*hits.Hit,
) (resolved []*hits.Hit, dropped int64, err ProtosessionError) {
	type candidate struct {
		hit      *hits.Hit
		settings *properties.Settings
		lookups  []int
	}
	var candidates []candidate
	var lookups []*IdentifierConflictRequest
	for _, annotation := range annotations {
		settings, getErr := o.settingsRegistry.GetByPropertyID(annotation.PropertyID)
		if getErr != nil {
			var notFoundError *properties.NotFoundError
			if errors.As(getErr, &notFoundError) {
				dropped++
				continue
			}
			return nil, 0, NewErrorCausingTaskRetry(getErr)
		}
		guard := o.identifierIsolationGuardFactory.New(settings)
		if settings.SessionJoinBySessionStamp {
			SetIsolatedSessionStamp(annotation, guard.IsolatedSessionStamp(annotation))
		}
		if settings.SessionJoinByUserID && annotation.UserID != nil {
			SetIsolatedUserIDStamp(annotation, guard.IsolatedUserID(annotation))
		}
		c := candidate{hit: annotation, settings: settings}
		for _, request := range GetConflictCheckRequests(annotation, settings) {
			c.lookups = append(c.lookups, len(lookups))
			lookups = append(lookups, request)
		}
		candidates = append(candidates, c)
	}

	var owners []*LookupIdentifierOwnerResponse
	if len(lookups) > 0 {
		owners = backend.LookupIdentifierOwners(ctx, lookups)
	}
	for _, c := range candidates {
		owner := c.hit.AuthoritativeClientID
		ambiguous := false
		var found hits.ClientID
		for _, index := range c.lookups {
			response := owners[index]
			if response.Err != nil {
				return nil, 0, NewErrorCausingTaskRetry(response.Err)
			}
			if !response.Found {
				continue
			}
			if found != "" && found != response.Owner {
				ambiguous = true
			}
			found = response.Owner
		}
		if ambiguous {
			dropped++
			continue
		}
		if found != "" {
			owner = found
		}
		c.hit.AuthoritativeClientID = owner
		guard := o.identifierIsolationGuardFactory.New(c.settings)
		SetIsolatedClientID(c.hit, guard.IsolatedClientID(c.hit))
		resolved = append(resolved, c.hit)
	}
	return resolved, dropped, nil
}

// buildAttachRequests groups resolved annotations per proto-session, in first-seen order.
func (o *Orchestrator) buildAttachRequests(resolved []*hits.Hit) []*AttachAnnotationsRequest {
	nowBucket := o.timingWheel.BucketNumber(o.timingWheel.CurrentTime())
	index := make(map[hits.ClientID]int)
	var requests []*AttachAnnotationsRequest
	for _, annotation := range resolved {
		id := GetIsolatedClientID(annotation)
		i, ok := index[id]
		if !ok {
			i = len(requests)
			index[id] = i
			requests = append(requests, &AttachAnnotationsRequest{ProtoSessionID: id, NowBucket: nowBucket})
		}
		requests[i].Annotations = append(requests[i].Annotations, annotation)
		requests[i].EventBuckets = append(requests[i].EventBuckets,
			o.timingWheel.BucketNumber(annotation.MustParsedRequest().ServerReceivedTime))
	}
	return requests
}

// claimAnnotationsForClose claims the proto-sessions about to be closed and appends their
// annotations after their hits. Claiming blocks further attaches, so every accepted annotation is
// part of the close; claiming again on retry returns the same annotations.
func (o *Orchestrator) claimAnnotationsForClose(ctx context.Context, batch [][]*hits.Hit) ([][]*hits.Hit, error) {
	backend, ok := o.backend.(AnnotationBackend)
	if !ok || len(batch) == 0 {
		return batch, nil
	}
	requests := make([]*ClaimForCloseRequest, len(batch))
	for i, protoSession := range batch {
		requests[i] = &ClaimForCloseRequest{ProtoSessionID: GetIsolatedClientID(protoSession[0])}
	}
	responses := backend.ClaimForClose(ctx, requests)
	withAnnotations := make([][]*hits.Hit, len(batch))
	for i, response := range responses {
		if response.Err != nil {
			return nil, response.Err
		}
		withAnnotations[i] = batch[i]
		if len(response.Annotations) == 0 {
			continue
		}
		annotations := make([]*hits.Hit, 0, len(response.Annotations))
		for _, annotation := range response.Annotations {
			if hits.IsAnnotation(annotation) {
				annotations = append(annotations, annotation)
			}
		}
		_, sorted := partitionAnnotations(annotations)
		combined := make([]*hits.Hit, 0, len(batch[i])+len(sorted))
		withAnnotations[i] = append(append(combined, batch[i]...), sorted...)
	}
	return withAnnotations, nil
}
