package splitter

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/schema"
	expr "github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// compiledCondition holds a compiled filter condition.
type compiledCondition struct {
	config  properties.ConditionConfig
	program *vm.Program
}

// filterModifier implements SessionModifier for event filtering.
type filterModifier struct {
	fields     []string
	conditions []compiledCondition
}

// Split implements SessionModifier.
// It evaluates filter conditions against events and removes matching/non-matching
// events depending on filter type (exclude/allow). For inactive conditions
// (testing mode), it sets event metadata instead of removing events.
func (f *filterModifier) Split(session *schema.Session) ([]*schema.Session, error) {
	if len(session.Events) == 0 {
		return []*schema.Session{session}, nil
	}

	// Filter events based on conditions
	filteredEvents := make([]*schema.Event, 0, len(session.Events))

	for _, event := range session.Events {
		// Build expr environment from configured fields
		env := f.buildEventEnvironment(event)

		shouldKeep := true

		// Evaluate all conditions
		for _, cond := range f.conditions {
			result, err := expr.Run(cond.program, env)
			if err != nil {
				// If evaluation fails, treat as non-matching
				continue
			}

			matched, ok := result.(bool)
			if !ok {
				continue
			}

			if !matched {
				continue
			}

			// Condition matched
			if cond.config.Active {
				// Active: apply filter logic
				switch cond.config.Type {
				case properties.FilterTypeExclude:
					// Exclude: remove matching events
					shouldKeep = false
				case properties.FilterTypeAllow:
					// Allow: keep only matching events, mark others for removal
					// Will be handled by checking if any allow conditions exist
				}
			} else {
				// Inactive (testing): set metadata
				event.Metadata["traffic_filter_name"] = cond.config.Name
			}
		}

		// Handle allow-type filters: if any allow conditions exist, only keep if matched
		hasActiveAllowCondition := false
		anyAllowMatched := false
		for _, cond := range f.conditions {
			if !cond.config.Active || cond.config.Type != properties.FilterTypeAllow {
				continue
			}
			hasActiveAllowCondition = true

			result, err := expr.Run(cond.program, env)
			if err != nil {
				continue
			}
			matched, ok := result.(bool)
			if ok && matched {
				anyAllowMatched = true
				break
			}
		}

		if hasActiveAllowCondition && !anyAllowMatched {
			shouldKeep = false
		}

		if shouldKeep {
			filteredEvents = append(filteredEvents, event)
		}
	}

	// If all events were filtered, return empty slice
	if len(filteredEvents) == 0 {
		return []*schema.Session{}, nil
	}

	// Update session with filtered events
	session.Events = filteredEvents
	return []*schema.Session{session}, nil
}

// buildEventEnvironment creates an expr environment from event field values.
func (f *filterModifier) buildEventEnvironment(event *schema.Event) map[string]any {
	env := make(map[string]any)
	for _, field := range f.fields {
		if value, ok := event.Values[field]; ok {
			// Convert to string for expr evaluation
			switch str := value.(type) {
			case string:
				env[field] = str
			case *string:
				if str != nil {
					env[field] = *str
				} else {
					env[field] = ""
				}
			default:
				// Try to convert other types to string
				env[field] = fmt.Sprintf("%v", value)
			}
		} else {
			env[field] = ""
		}
	}
	return env
}

// New creates a new filter modifier from configuration.
// Returns a noop modifier when config has no active conditions.
func NewFilter(config properties.FiltersConfig) (SessionModifier, error) {
	if len(config.Conditions) == 0 {
		return &filterModifier{fields: []string{}, conditions: []compiledCondition{}}, nil
	}

	compiled := make([]compiledCondition, 0, len(config.Conditions))

	// Compile all expressions at startup for fail-fast behavior
	for _, cond := range config.Conditions {
		// Prepare compile options with custom functions
		opts := append(
			FunctionOptions(),
			expr.AllowUndefinedVariables(),
		)

		program, err := expr.Compile(cond.Expression, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to compile filter expression %q: %w", cond.Name, err)
		}

		compiled = append(compiled, compiledCondition{
			config:  cond,
			program: program,
		})
	}

	return &filterModifier{
		fields:     config.Fields,
		conditions: compiled,
	}, nil
}
