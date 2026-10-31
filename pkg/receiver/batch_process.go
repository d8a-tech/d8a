package receiver

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/transformations"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// HitBatchProcessingRule processes a batch before validation, returning only retained hits.
// original is the request as received, before per-hit processing such as IP masking.
type HitBatchProcessingRule func(
	p protocol.Protocol, original *hits.ParsedRequest, batch []*hits.Hit,
) ([]*hits.Hit, error)

const normalizedRequestMetadataKey = "receiver.normalized_request"

var transformationErrorCounter metric.Int64Counter

func init() {
	transformationErrorCounter, _ = otel.GetMeterProvider().Meter("receiver").Int64Counter(
		"receiver.transformations.errors",
		metric.WithDescription("Hits dropped because an event transformation failed"),
	)
}

// programMemo maps configuration pointers to compiled programs. Settings registries hand
// out the same *TransformationsConfig until the configuration changes, so a batch costs one
// map lookup instead of hashing the rules. Entries keep their config alive; the map is
// bounded and reset when full.
type programMemo struct {
	sync.RWMutex
	programs map[*properties.TransformationsConfig]transformations.Program
}

const maxMemoizedPrograms = 1024

func (m *programMemo) get(config *properties.TransformationsConfig) (transformations.Program, error) {
	m.RLock()
	cached, ok := m.programs[config]
	m.RUnlock()
	if ok {
		return cached, nil
	}
	compiled, err := transformations.Compile(*config)
	if err != nil {
		return nil, err
	}
	m.Lock()
	if len(m.programs) >= maxMemoizedPrograms {
		clear(m.programs)
	}
	m.programs[config] = compiled
	m.Unlock()
	return compiled, nil
}

// errorLogLimiter keeps transformation failures visible in logs without flooding them.
type errorLogLimiter struct {
	sync.Mutex
	last time.Time
}

func (l *errorLogLimiter) allow() bool {
	l.Lock()
	defer l.Unlock()
	if time.Since(l.last) < 10*time.Second {
		return false
	}
	l.last = time.Now()
	return true
}

// NewTransformationBatchProcessingRule captures one compiled plan per property per batch.
// A failing rule drops only the affected hit (fail-closed): passing it on untransformed
// could keep data a rule is meant to remove.
func NewTransformationBatchProcessingRule(
	settings properties.SettingsRegistry, deps transformations.ExtractionDependencies,
) HitBatchProcessingRule {
	memo := &programMemo{programs: make(map[*properties.TransformationsConfig]transformations.Program)}
	limiter := &errorLogLimiter{}
	report := func(propertyID, stage string, rule int) {
		transformationErrorCounter.Add(context.Background(), 1, metric.WithAttributes(
			attribute.String("stage", stage), attribute.String("rule", strconv.Itoa(rule)),
		))
		if limiter.allow() {
			logrus.WithFields(logrus.Fields{"property_id": propertyID, "stage": stage, "rule": rule}).
				Warn("event transformation failed; hit dropped")
		}
	}
	return func(p protocol.Protocol, original *hits.ParsedRequest, batch []*hits.Hit) ([]*hits.Hit, error) {
		plans := make(map[string]transformations.Program)
		snapshots := make(map[string]*properties.Settings)
		broken := make(map[string]bool)
		configured := false
		for _, hit := range batch {
			if hit == nil || hit.Request == nil {
				return nil, errors.New("transformations: missing parsed hit")
			}
			if _, ok := snapshots[hit.PropertyID]; ok {
				continue
			}
			if settings == nil {
				return nil, errors.New("transformations: settings unavailable")
			}
			current, err := settings.GetByPropertyID(hit.PropertyID)
			if err != nil || current == nil {
				return nil, errors.New("transformations: settings lookup failed")
			}
			snapshot := *current
			snapshot.ExcludedURLParams = current.ExcludedURLParamsSafe()
			snapshots[hit.PropertyID] = &snapshot
			if current.Transformations == nil || len(current.Transformations.Rules) == 0 {
				continue
			}
			configured = true
			plan, err := memo.get(current.Transformations)
			if err != nil {
				broken[hit.PropertyID] = true
				continue
			}
			plans[hit.PropertyID] = plan
		}
		if !configured {
			return batch, nil
		}
		originalIP := ""
		if original != nil {
			originalIP = original.IP
		}
		retained := make([]*hits.Hit, 0, len(batch))
		for _, source := range batch {
			if broken[source.PropertyID] {
				report(source.PropertyID, "compile", 0)
				continue
			}
			// Normalize the entire batch, including unconfigured properties: their
			// shared body may contain events dropped by another property's rules.
			hit := source.Copy()
			hit.Request.Body = nil
			if hit.Metadata == nil {
				hit.Metadata = make(map[string]string)
			}
			hit.Metadata[normalizedRequestMetadataKey] = "true"
			if plan := plans[hit.PropertyID]; plan != nil {
				keep, err := plan.Apply(transformations.Input{
					Protocol: p.ID(), Hit: &hit, Settings: snapshots[hit.PropertyID],
					OriginalIP: originalIP, Dependencies: deps,
				})
				if err != nil {
					var failure *transformations.Error
					stage, rule := "unknown", 0
					if errors.As(err, &failure) {
						stage, rule = failure.Stage, failure.Rule
					}
					report(hit.PropertyID, stage, rule)
					continue
				}
				if !keep {
					continue
				}
			}
			retained = append(retained, &hit)
		}
		return retained, nil
	}
}
