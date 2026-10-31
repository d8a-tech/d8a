package transformations

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"unicode/utf8"

	"github.com/d8a-tech/d8a/pkg/expressions"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
)

// A mutex-protected FIFO keeps at most MaxCachedPrograms immutable plans.
// Compile is startup/refresh work; replace with a concurrent cache if it becomes a hot path.
var compiledCache = struct {
	sync.Mutex
	programs map[[32]byte]*program
	order    [][32]byte
}{programs: make(map[[32]byte]*program)}

type program struct{ rules []compiledRule }
type compiledRule struct {
	predicate *vm.Program
	fields    []string
	actions   []compiledAction
}
type compiledAction struct {
	kind   string
	target properties.TransformationFieldReference
	source properties.TransformationFieldReference
	value  any
}

// Compile validates and snapshots executable configuration. The bounded cache does
// not retain caller-owned pointers. Rule names do not affect executable identity.
func Compile(config properties.TransformationsConfig) (Program, error) {
	if len(config.Rules) > MaxRules {
		return nil, fmt.Errorf("transformations: maximum %d rules", MaxRules)
	}
	identity := properties.TransformationsConfig{Rules: make([]properties.TransformationRuleConfig, len(config.Rules))}
	for i, rule := range config.Rules {
		if utf8.RuneCountInString(rule.Name) > MaxNameLength {
			return nil, fmt.Errorf("transformations rule %d: name exceeds %d characters", i+1, MaxNameLength)
		}
		identity.Rules[i] = rule
		identity.Rules[i].Name = ""
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return nil, fmt.Errorf("transformations: invalid literal encoding")
	}
	key := sha256.Sum256(encoded)
	compiledCache.Lock()
	defer compiledCache.Unlock()
	if cached := compiledCache.programs[key]; cached != nil {
		return cached, nil
	}
	result := &program{rules: make([]compiledRule, len(config.Rules))}
	for i, rule := range config.Rules {
		compiled, err := compileRule(rule)
		if err != nil {
			return nil, fmt.Errorf("transformations rule %d: %w", i+1, err)
		}
		result.rules[i] = compiled
	}
	if len(compiledCache.order) == MaxCachedPrograms {
		delete(compiledCache.programs, compiledCache.order[0])
		compiledCache.order = compiledCache.order[1:]
	}
	compiledCache.programs[key] = result
	compiledCache.order = append(compiledCache.order, key)
	return result, nil
}

func compileRule(rule properties.TransformationRuleConfig) (compiledRule, error) {
	var result compiledRule
	if rule.Expression == "" || utf8.RuneCountInString(rule.Expression) > MaxExpressionLength {
		return result, fmt.Errorf("expression must contain 1 to %d characters", MaxExpressionLength)
	}
	if len(rule.Actions) == 0 || len(rule.Actions) > MaxActions {
		return result, fmt.Errorf("expected 1 to %d actions", MaxActions)
	}
	env := make(map[string]any, len(fieldNames)+2)
	for _, field := range fieldNames {
		env[field] = ""
	}
	env["params"] = map[string]string{}
	env["numeric_params"] = map[string]float64{}
	options := append(expressions.FunctionOptions(), expr.Env(env), expr.AsBool())
	predicate, err := expr.Compile(rule.Expression, options...)
	if err != nil {
		return result, fmt.Errorf("invalid boolean expression: %w", err)
	}
	result.predicate = predicate
	for field := range env {
		found := ast.Find(predicate.Node(), func(node ast.Node) bool {
			id, ok := node.(*ast.IdentifierNode)
			return ok && (id.Value == field || id.Value == "$env")
		})
		if found != nil {
			result.fields = append(result.fields, field)
		}
	}
	seen := make(map[string]bool)
	for i, action := range rule.Actions {
		compiled, err := compileAction(action, len(rule.Actions))
		if err != nil {
			return result, fmt.Errorf("action %d: %w", i+1, err)
		}
		key := compiled.target.Field
		if compiled.target.Parameter != "" {
			key = "parameter:" + compiled.target.Parameter
		}
		if compiled.kind != "drop" && seen[key] {
			return result, fmt.Errorf("action %d: duplicate canonical target", i+1)
		}
		seen[key] = true
		result.actions = append(result.actions, compiled)
	}
	return result, nil
}

func normalizeReference(ref *properties.TransformationFieldReference) (properties.TransformationFieldReference, error) {
	if ref == nil {
		return properties.TransformationFieldReference{}, fmt.Errorf("missing field reference")
	}
	result := *ref
	if result.Parameter != "" {
		if result.Field != "" || (result.Type != "string" && result.Type != "number") ||
			utf8.RuneCountInString(result.Parameter) > MaxNameLength {
			return result, fmt.Errorf("parameter requires a name and string/number type, without field")
		}
		return result, nil
	}
	result.Field = canonicalField(result.Field)
	for _, field := range fieldNames {
		if result.Field == field && result.Type == "" {
			return result, nil
		}
	}
	return result, fmt.Errorf("unknown field reference")
}

func referenceType(ref properties.TransformationFieldReference) string {
	if ref.Parameter != "" {
		return ref.Type
	}
	return "string"
}

func compileAction(action properties.TransformationActionConfig, count int) (compiledAction, error) {
	result := compiledAction{kind: action.Type}
	if action.Type == "drop" {
		if count != 1 || action.Target != nil || action.Value != nil || action.Source != nil {
			return result, fmt.Errorf("drop must be exclusive and have no target, value or source")
		}
		return result, nil
	}
	target, err := normalizeReference(action.Target)
	if err != nil {
		return result, err
	}
	if target.Parameter == "" && !writable(target.Field) {
		return result, fmt.Errorf("target is read-only")
	}
	result.target = target
	switch action.Type {
	case "set":
		if action.Source != nil {
			return result, fmt.Errorf("set cannot have a source")
		}
		result.value, err = literalValue(action.Value, referenceType(target))
	case "copy":
		if action.Value != nil {
			return result, fmt.Errorf("copy cannot have a literal")
		}
		result.source, err = normalizeReference(action.Source)
		if err == nil && referenceType(result.source) != referenceType(target) {
			err = fmt.Errorf("copy types differ")
		}
	case "remove":
		if target.Field == "name" || action.Source != nil || action.Value != nil {
			err = fmt.Errorf("remove requires a removable target and no source or value")
		}
	default:
		err = fmt.Errorf("unknown action type")
	}
	return result, err
}

func literalValue(value *properties.TransformationLiteral, kind string) (any, error) {
	if value == nil || (value.String == nil) == (value.Number == nil) {
		return nil, fmt.Errorf("expected one typed literal")
	}
	if kind == "string" && value.String != nil && utf8.RuneCountInString(*value.String) <= MaxValueLength {
		return *value.String, nil
	}
	if kind == "number" && value.Number != nil && !math.IsNaN(*value.Number) && !math.IsInf(*value.Number, 0) {
		return *value.Number, nil
	}
	return nil, fmt.Errorf("literal has wrong type, is nonfinite or exceeds %d characters", MaxValueLength)
}
