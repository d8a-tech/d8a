package receiver

import (
	"errors"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/transformations"
)

// HitBatchProcessingRule processes a batch before validation, returning only retained hits.
type HitBatchProcessingRule func(protocol.Protocol, []*hits.Hit) ([]*hits.Hit, error)

const normalizedRequestMetadataKey = "receiver.normalized_request"

// NewTransformationBatchProcessingRule captures one compiled plan per property per batch.
func NewTransformationBatchProcessingRule(
	settings properties.SettingsRegistry, deps transformations.ExtractionDependencies,
) HitBatchProcessingRule {
	return func(p protocol.Protocol, batch []*hits.Hit) ([]*hits.Hit, error) {
		plans := make(map[string]transformations.Program)
		snapshots := make(map[string]*properties.Settings)
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
			plan, err := transformations.Compile(*current.Transformations)
			if err != nil {
				return nil, errors.New("transformations: compilation failed")
			}
			plans[hit.PropertyID] = plan
			configured = true
		}
		if !configured {
			return batch, nil
		}
		retained := make([]*hits.Hit, 0, len(batch))
		for _, source := range batch {
			// Normalize the entire batch, including unconfigured properties: their
			// shared body may contain events dropped by another property's rules.
			hit := source.Copy()
			hit.Request.Body = nil
			if hit.Metadata == nil {
				hit.Metadata = make(map[string]string)
			}
			hit.Metadata[normalizedRequestMetadataKey] = "true"
			if plan := plans[hit.PropertyID]; plan != nil {
				keep, err := plan.Apply(p.ID(), &hit, snapshots[hit.PropertyID], deps)
				if err != nil {
					return nil, errors.New("transformations: execution failed")
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
