package filter

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/d8a-tech/d8a/pkg/splitter"
	expr "github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// compiledCondition holds a compiled filter condition.
type compiledCondition struct {
	config  ConditionConfig
	program *vm.Program
}

// filterModifier implements splitter.SessionModifier for event filtering.
type filterModifier struct {
	fields     []string
	conditions []compiledCondition
}

// Split implements splitter.SessionModifier.
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
				case FilterTypeExclude:
					// Exclude: remove matching events
					shouldKeep = false
				case FilterTypeAllow:
					// Allow: keep only matching events, mark others for removal
					// Will be handled by checking if any allow conditions exist
				}
			} else {
				// Inactive (testing): set metadata
				event.Metadata["engaged_filter_name"] = cond.config.Name
			}
		}

		// Handle allow-type filters: if any allow conditions exist, only keep if matched
		hasActiveAllowCondition := false
		anyAllowMatched := false
		for _, cond := range f.conditions {
			if !cond.config.Active || cond.config.Type != FilterTypeAllow {
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
			if str, ok := value.(string); ok {
				env[field] = str
			} else if str, ok := value.(*string); ok {
				if str != nil {
					env[field] = *str
				} else {
					env[field] = ""
				}
			} else {
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
func New(config FiltersConfig) (splitter.SessionModifier, error) {
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
