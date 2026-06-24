package receiver

import (
	"errors"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/protocol"
)

// HitProcessingRule defines the interface for processing hits.
type HitProcessingRule interface {
	Process(p protocol.Protocol, hit *hits.Hit) error
}

type multipleHitProcessingRule struct {
	rules []HitProcessingRule
}

func (r *multipleHitProcessingRule) Process(p protocol.Protocol, hit *hits.Hit) error {
	var errs []error
	for _, rule := range r.rules {
		if err := rule.Process(p, hit); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("multiple hit processing rules failed: %w", errors.Join(errs...))
	}
	return nil
}

// NewMultipleHitProcessingRule creates a new processing rule that combines multiple rules.
func NewMultipleHitProcessingRule(rules ...HitProcessingRule) HitProcessingRule {
	return &multipleHitProcessingRule{rules: rules}
}

type simpleHitProcessingRule struct {
	rule func(protocol.Protocol, *hits.Hit) error
}

func (r *simpleHitProcessingRule) Process(p protocol.Protocol, hit *hits.Hit) error {
	return r.rule(p, hit)
}

// NewSimpleHitProcessingRule creates a new processing rule from a simple function.
func NewSimpleHitProcessingRule(rule func(protocol.Protocol, *hits.Hit) error) HitProcessingRule {
	return &simpleHitProcessingRule{rule: rule}
}

// NoopHitProcessingRule does not change hits.
var NoopHitProcessingRule = NewSimpleHitProcessingRule(func(protocol.Protocol, *hits.Hit) error {
	return nil
})
