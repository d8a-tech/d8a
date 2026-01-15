package receiver

import (
	"errors"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
)

// HitValidatingRule defines the interface for validating hits.
type HitValidatingRule interface {
	Validate(p protocol.Protocol, hit *hits.Hit) error
}

type multipleHitValidatingRule struct {
	rules []HitValidatingRule
}

func (r *multipleHitValidatingRule) Validate(p protocol.Protocol, hit *hits.Hit) error {
	var errs []error
	for _, rule := range r.rules {
		if err := rule.Validate(p, hit); err != nil {
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
	rule func(protocol.Protocol, *hits.Hit) error
}

func (r *simpleHitValidatingRule) Validate(p protocol.Protocol, hit *hits.Hit) error {
	return r.rule(p, hit)
}

// NewSimpleHitValidatingRule creates a new validating rule from a simple function.
func NewSimpleHitValidatingRule(rule func(protocol.Protocol, *hits.Hit) error) HitValidatingRule {
	return &simpleHitValidatingRule{rule: rule}
}

// ClientIDNotEmpty validates that both ClientID and AuthoritativeClientID are not empty.
var ClientIDNotEmpty = NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
	if hit.ClientID == "" || hit.AuthoritativeClientID == "" {
		return fmt.Errorf("hit.ClientID and hit.AuthoritativeClientID can not be empty")
	}
	return nil
})

// PropertyIDNotEmpty validates that PropertyID is not empty.
var PropertyIDNotEmpty = NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
	if hit.PropertyID == "" {
		return fmt.Errorf("hit.PropertyID can not be empty")
	}
	return nil
})

// HitHeadersNotEmpty validates that Headers are not empty.
var HitHeadersNotEmpty = NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
	if len(hit.MustParsedRequest().Headers) == 0 {
		return fmt.Errorf("hit.Headers can not be empty")
	}
	return nil
})

// HitQueryParamsNotNil validates that QueryParams are not nil.
var HitQueryParamsNotNil = NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
	if hit.MustParsedRequest().QueryParams == nil {
		return fmt.Errorf("hit.QueryParams can not be nil")
	}
	return nil
})

// HitHostNotEmpty validates that Host is not empty.
var HitHostNotEmpty = NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
	if hit.MustParsedRequest().Host == "" {
		return fmt.Errorf("hit.Host can not be empty")
	}
	return nil
})

// HitPathNotEmpty validates that Path is not empty.
var HitPathNotEmpty = NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
	if hit.MustParsedRequest().Path == "" {
		return fmt.Errorf("hit.Path can not be empty")
	}
	return nil
})

// HitMethodNotEmpty validates that Method is not empty.
var HitMethodNotEmpty = NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
	if hit.MustParsedRequest().Method == "" {
		return fmt.Errorf("hit.Method can not be empty")
	}
	return nil
})

// HitBodyNotNil validates that Body is not nil.
var HitBodyNotNil = NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
	if hit.MustParsedRequest().Body == nil {
		return fmt.Errorf("hit.Body can not be nil")
	}
	return nil
})

// TotalHitSizeDoesNotExceed validates that the total size of the hit does not exceed the max allowed size.
func TotalHitSizeDoesNotExceed(maxHitSizeBytes uint32) HitValidatingRule {
	return NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
		if hit.Size() > maxHitSizeBytes {
			return fmt.Errorf("hit size exceeds max allowed size of %d bytes", maxHitSizeBytes)
		}
		return nil
	})
}

// EventNameNotEmpty validates that EventName is not empty.
var EventNameNotEmpty = NewSimpleHitValidatingRule(func(_ protocol.Protocol, hit *hits.Hit) error {
	if hit.EventName == "" {
		return fmt.Errorf("hit.EventName can not be empty")
	}
	return nil
})

// PropertyProtocolMatchesTheEndpointProtocol checks if the protocols of endpoint and property match.
func PropertyProtocolMatchesTheEndpointProtocol(settings properties.SettingsRegistry) HitValidatingRule {
	return NewSimpleHitValidatingRule(func(p protocol.Protocol, hit *hits.Hit) error {
		settings, err := settings.GetByPropertyID(hit.PropertyID)
		if err != nil {
			return err
		}
		if settings.ProtocolID != p.ID() {
			return fmt.Errorf("property protocol %s does not match endpoint protocol %s", settings.ProtocolID, p.ID())
		}
		return nil
	})
}

// HitValidatingRuleSet returns a complete set of validation rules for hits.
func HitValidatingRuleSet(
	maxHitSizeBytes uint32,
	settings properties.SettingsRegistry,
) HitValidatingRule {
	return NewMultipleHitValidatingRule(
		ClientIDNotEmpty,
		PropertyIDNotEmpty,
		HitHeadersNotEmpty,
		HitQueryParamsNotNil,
		HitHostNotEmpty,
		HitPathNotEmpty,
		HitMethodNotEmpty,
		HitBodyNotNil,
		TotalHitSizeDoesNotExceed(maxHitSizeBytes),
		PropertyProtocolMatchesTheEndpointProtocol(settings),
		EventNameNotEmpty,
	)
}
