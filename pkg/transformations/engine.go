package transformations

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/expr-lang/expr"
)

// Program is an immutable, concurrency-safe compiled ruleset. Apply mutates an
// exclusively owned hit; callers must discard the affected request on error.
type Program interface {
	Apply(
		protocolID string, hit *hits.Hit, settings *properties.Settings, deps ExtractionDependencies,
	) (keep bool, err error)
}

// Apply implements Program.
func (p *program) Apply(
	protocolID string, hit *hits.Hit, settings *properties.Settings, deps ExtractionDependencies,
) (bool, error) {
	if hit == nil || hit.Request == nil {
		return false, fmt.Errorf("transformations: missing parsed hit")
	}
	view := newEventView(protocolID, hit, settings, deps)
	for i, rule := range p.rules {
		env, err := view.environment(rule.fields)
		if err != nil {
			return false, fmt.Errorf("transformations rule %d: extraction failed", i+1)
		}
		matched, err := expr.Run(rule.predicate, env)
		if err != nil {
			return false, fmt.Errorf("transformations rule %d: predicate evaluation failed", i+1)
		}
		match, ok := matched.(bool)
		if !ok {
			return false, fmt.Errorf("transformations rule %d: predicate did not return boolean", i+1)
		}
		if !match {
			continue
		}
		if rule.actions[0].kind == "drop" {
			return false, nil
		}
		if err := applyActions(view, rule.actions); err != nil {
			return false, fmt.Errorf("transformations rule %d: source extraction failed", i+1)
		}
	}
	return true, nil
}

func applyActions(view *eventView, actions []compiledAction) error {
	values := make([]any, len(actions))
	for i := range actions {
		action := &actions[i]
		values[i] = action.value
		if view.wireKey(action.target) == "" {
			continue
		}
		if action.kind == "copy" {
			value, err := view.get(action.source)
			if err != nil {
				return err
			}
			values[i] = value
		}
	}
	for i := range actions {
		action := &actions[i]
		if action.kind == "copy" && values[i] == nil {
			continue
		}
		view.write(action.target, values[i], action.kind == "remove")
	}
	return nil
}
