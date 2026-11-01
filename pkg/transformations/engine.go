package transformations

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/d8a-tech/d8a/pkg/hits"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/expr-lang/expr"
)

// Failure stages reported by Error.
const (
	StageExtraction = "extraction"
	StagePredicate  = "predicate"
	StageAction     = "action"
)

// Error is a payload-free failure: compiler and extraction messages can contain literals.
type Error struct {
	Stage string
	// Rule is the 1-based index of the failing rule.
	Rule int
}

func (e *Error) Error() string {
	return fmt.Sprintf("transformations rule %d: %s failed", e.Rule, e.Stage)
}

// Input is everything one hit's transformation can see.
type Input struct {
	Protocol string
	// Hit is mutated in place; callers must own it exclusively and discard it on error.
	Hit      *hits.Hit
	Settings *properties.Settings
	// OriginalIP is the unmasked client IP of the request. It is readable in conditions
	// only (ip_address) and is never written to the hit.
	OriginalIP   string
	Dependencies ExtractionDependencies
}

// Program is an immutable, concurrency-safe compiled ruleset.
type Program interface {
	Apply(input Input) (keep bool, err error)
}

// Apply implements Program.
func (p *program) Apply(input Input) (bool, error) {
	if input.Hit == nil || input.Hit.Request == nil {
		return false, &Error{Stage: StageExtraction}
	}
	view := newEventView(input)
	env := map[string]any{lazyFieldFunction: view.lookup}
	for i, rule := range p.rules {
		view.failed = false
		matched, err := expr.Run(rule.predicate, env)
		if err != nil {
			if view.failed {
				return false, &Error{Stage: StageExtraction, Rule: i + 1}
			}
			return false, &Error{Stage: StagePredicate, Rule: i + 1}
		}
		match, ok := matched.(bool)
		if !ok {
			return false, &Error{Stage: StagePredicate, Rule: i + 1}
		}
		if !match {
			continue
		}
		if rule.actions[0].kind == properties.TransformationActionDrop {
			return false, nil
		}
		if stage := applyActions(view, rule.actions); stage != "" {
			return false, &Error{Stage: stage, Rule: i + 1}
		}
	}
	return true, nil
}

// applyActions evaluates every value against the pre-rule state, then writes in order and
// invalidates memoized reads once, after the last write.
func applyActions(view *eventView, actions []compiledAction) string {
	defer view.invalidate()
	values := make([]any, len(actions))
	skip := make([]bool, len(actions))
	for i := range actions {
		action := &actions[i]
		if action.kind == properties.TransformationActionRemove {
			continue
		}
		if action.kind == properties.TransformationActionReplace {
			value, ok, err := view.replace(action)
			if err != nil {
				return StageExtraction
			}
			values[i], skip[i] = value, !ok
			continue
		}
		value, ok, err := view.evaluate(action.value, referenceType(action.target))
		if err != nil {
			return StageExtraction
		}
		values[i], skip[i] = value, !ok
	}
	for i := range actions {
		action := &actions[i]
		if skip[i] {
			continue
		}
		if err := view.write(action.target, values[i], action.kind == properties.TransformationActionRemove); err != nil {
			return StageAction
		}
	}
	return ""
}

// evaluate resolves a literal or template. ok=false skips the action: a template that is a
// single reference to a missing value leaves the target untouched.
func (v *eventView) evaluate(value compiledValue, kind string) (any, bool, error) {
	if value.template == nil {
		return value.literal, true, nil
	}
	if len(value.template) == 1 && value.template[0].ref != nil {
		resolved, err := v.get(*value.template[0].ref)
		if err != nil || resolved == nil {
			return nil, false, err
		}
		if kind == "number" {
			number, ok := resolved.(float64)
			return number, ok, nil
		}
		return formatValue(resolved), true, nil
	}
	var builder strings.Builder
	for _, part := range value.template {
		if part.ref == nil {
			builder.WriteString(part.text)
			continue
		}
		resolved, err := v.get(*part.ref)
		if err != nil {
			return nil, false, err
		}
		builder.WriteString(formatValue(resolved))
	}
	return builder.String(), true, nil
}

func formatValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprint(typed)
	}
}
