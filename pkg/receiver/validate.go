package receiver

import (
	"errors"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/hits"
)

// HitValidatingRule defines the interface for validating hits.
type HitValidatingRule interface {
	Validate(*hits.Hit) error
}

type multipleHitValidatingRule struct {
	rules []HitValidatingRule
}

func (r *multipleHitValidatingRule) Validate(hit *hits.Hit) error {
	var errs []error
	for _, rule := range r.rules {
		if err := rule.Validate(hit); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("multiple hit validating rules failed: %w", errors.Join(errs...))
	}
	return nil
}

// NewMultipleHitValidatingRule creates a new validating rule that combines multiple rules.
func NewMultipleHitValidatingRule(rules ...HitValidatingRule) HitValidatingRule {
	return &multipleHitValidatingRule{rules: rules}
}

type simpleHitValidatingRule struct {
	rule func(*hits.Hit) error
}

func (r *simpleHitValidatingRule) Validate(hit *hits.Hit) error {
	return r.rule(hit)
}

// NewSimpleHitValidatingRule creates a new validating rule from a simple function.
func NewSimpleHitValidatingRule(rule func(*hits.Hit) error) HitValidatingRule {
	return &simpleHitValidatingRule{rule: rule}
}

// ClientIDNotEmpty validates that both ClientID and AuthoritativeClientID are not empty.
var ClientIDNotEmpty = NewSimpleHitValidatingRule(func(hit *hits.Hit) error {
	if hit.ClientID == "" || hit.AuthoritativeClientID == "" {
		return fmt.Errorf("hit.ClientID and hit.AuthoritativeClientID can not be empty")
	}
	return nil
})

// PropertyIDNotEmpty validates that PropertyID is not empty.
var PropertyIDNotEmpty = NewSimpleHitValidatingRule(func(hit *hits.Hit) error {
	if hit.PropertyID == "" {
		return fmt.Errorf("hit.PropertyID can not be empty")
	}
	return nil
})

// TimestampNotZero validates that Timestamp is not zero.
var TimestampNotZero = NewSimpleHitValidatingRule(func(hit *hits.Hit) error {
	if hit.Timestamp.IsZero() {
		return fmt.Errorf("hit.Timestamp can not be zero")
	}
	return nil
})

// HitHeadersNotEmpty validates that Headers are not empty.
var HitHeadersNotEmpty = NewSimpleHitValidatingRule(func(hit *hits.Hit) error {
	if len(hit.Headers) == 0 {
		return fmt.Errorf("hit.Headers can not be empty")
	}
	return nil
})

// HitQueryParamsNotNil validates that QueryParams are not nil.
var HitQueryParamsNotNil = NewSimpleHitValidatingRule(func(hit *hits.Hit) error {
	if hit.QueryParams == nil {
		return fmt.Errorf("hit.QueryParams can not be nil")
	}
	return nil
})

// HitHostNotEmpty validates that Host is not empty.
var HitHostNotEmpty = NewSimpleHitValidatingRule(func(hit *hits.Hit) error {
	if hit.Host == "" {
		return fmt.Errorf("hit.Host can not be empty")
	}
	return nil
})

// HitPathNotEmpty validates that Path is not empty.
var HitPathNotEmpty = NewSimpleHitValidatingRule(func(hit *hits.Hit) error {
	if hit.Path == "" {
		return fmt.Errorf("hit.Path can not be empty")
	}
	return nil
})

// HitMethodNotEmpty validates that Method is not empty.
var HitMethodNotEmpty = NewSimpleHitValidatingRule(func(hit *hits.Hit) error {
	if hit.Method == "" {
		return fmt.Errorf("hit.Method can not be empty")
	}
	return nil
})

// HitBodyNotNil validates that Body is not nil.
var HitBodyNotNil = NewSimpleHitValidatingRule(func(hit *hits.Hit) error {
	if hit.Body == nil {
		return fmt.Errorf("hit.Body can not be nil")
	}
	return nil
})

// HitValidatingRuleSet returns a complete set of validation rules for hits.
func HitValidatingRuleSet() HitValidatingRule {
	return NewMultipleHitValidatingRule(
		ClientIDNotEmpty,
		PropertyIDNotEmpty,
		TimestampNotZero,
		HitHeadersNotEmpty,
		HitQueryParamsNotNil,
		HitHostNotEmpty,
		HitPathNotEmpty,
		HitMethodNotEmpty,
		HitBodyNotNil,
	)
}
