package transformations

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/d8a-tech/d8a/pkg/expressions"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
)

// lazyFieldFunction is the env function compiled expressions call instead of reading
// precomputed identifiers, so && and || short-circuit before expensive extraction.
const lazyFieldFunction = "__field"

// Map-valued namespaces available in expressions besides catalog fields.
const (
	paramsNamespace        = "params"
	numericParamsNamespace = "numeric_params"
	urlParamsNamespace     = "url_params"
)

// A mutex-protected FIFO keeps at most MaxCachedPrograms immutable plans. The lock only
// guards the map: compilation itself runs outside it.
var compiledCache = struct {
	sync.Mutex
	programs map[[32]byte]*program
	order    [][32]byte
}{programs: make(map[[32]byte]*program)}

type program struct{ rules []compiledRule }
type compiledRule struct {
	predicate *vm.Program
	actions   []compiledAction
}
type compiledAction struct {
	kind   string
	target properties.TransformationFieldReference
	value  compiledValue
}

// compiledValue is either a literal or a parsed template.
type compiledValue struct {
	literal  any
	template []templatePart
}

type templatePart struct {
	text string
	ref  *properties.TransformationFieldReference
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
	cached := compiledCache.programs[key]
	compiledCache.Unlock()
	if cached != nil {
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
	compiledCache.Lock()
	defer compiledCache.Unlock()
	if existing := compiledCache.programs[key]; existing != nil {
		return existing, nil
	}
	if len(compiledCache.order) == MaxCachedPrograms {
		delete(compiledCache.programs, compiledCache.order[0])
		compiledCache.order = compiledCache.order[1:]
	}
	compiledCache.programs[key] = result
	compiledCache.order = append(compiledCache.order, key)
	return result, nil
}

// typedEnvironment declares every readable name with its type, for validation only.
func typedEnvironment() map[string]any {
	catalog := loadCatalog()
	env := make(map[string]any, len(catalog.fields)+3)
	for name, info := range catalog.fields {
		if info.capability.Type == "number" {
			env[name] = float64(0)
		} else {
			env[name] = ""
		}
	}
	env[paramsNamespace] = map[string]string{}
	env[numericParamsNamespace] = map[string]float64{}
	env[urlParamsNamespace] = map[string]string{}
	return env
}

func runtimeNames() map[string]bool {
	names := make(map[string]bool)
	for name := range typedEnvironment() {
		names[name] = true
	}
	return names
}

// lazyPatcher rewrites identifiers of readable names into __field("name") calls.
type lazyPatcher struct{ names map[string]bool }

func (p lazyPatcher) Visit(node *ast.Node) {
	identifier, ok := (*node).(*ast.IdentifierNode)
	if !ok || !p.names[identifier.Value] {
		return
	}
	ast.Patch(node, &ast.CallNode{
		Callee:    &ast.IdentifierNode{Value: lazyFieldFunction},
		Arguments: []ast.Node{&ast.StringNode{Value: identifier.Value}},
	})
}

func compileRule(rule properties.TransformationRuleConfig) (compiledRule, error) {
	var result compiledRule
	if rule.Expression == "" || utf8.RuneCountInString(rule.Expression) > MaxExpressionLength {
		return result, fmt.Errorf("expression must contain 1 to %d characters", MaxExpressionLength)
	}
	if len(rule.Actions) == 0 || len(rule.Actions) > MaxActions {
		return result, fmt.Errorf("expected 1 to %d actions", MaxActions)
	}
	// Type-check against declared field types first: lazy calls return untyped values.
	typed := append(expressions.FunctionOptions(), expr.Env(typedEnvironment()), expr.AsBool())
	if _, err := expr.Compile(rule.Expression, typed...); err != nil {
		return result, fmt.Errorf("invalid boolean expression: %w", err)
	}
	runtimeEnv := map[string]any{lazyFieldFunction: func(string) (any, error) { return nil, nil }}
	lazy := append(expressions.FunctionOptions(),
		expr.Env(runtimeEnv), expr.AsBool(), expr.Patch(lazyPatcher{names: runtimeNames()}))
	predicate, err := expr.Compile(rule.Expression, lazy...)
	if err != nil {
		return result, fmt.Errorf("invalid boolean expression: %w", err)
	}
	result.predicate = predicate
	seen := make(map[string]bool)
	pageLocation, pageDependent := false, false
	for i, action := range rule.Actions {
		compiled, err := compileAction(action, len(rule.Actions))
		if err != nil {
			return result, fmt.Errorf("action %d: %w", i+1, err)
		}
		if compiled.kind != "drop" {
			key := referenceKey(compiled.target)
			if seen[key] {
				return result, fmt.Errorf("action %d: duplicate canonical target", i+1)
			}
			seen[key] = true
			pageLocation = pageLocation || compiled.target.Field == "page_location"
			pageDependent = pageDependent || isPageDependent(compiled.target)
		}
		result.actions = append(result.actions, compiled)
	}
	if pageLocation && pageDependent {
		return result, fmt.Errorf("page_location cannot be written together with URL parts in one rule")
	}
	return result, nil
}

func referenceKey(ref properties.TransformationFieldReference) string {
	switch {
	case ref.Parameter != "":
		return "parameter:" + ref.Parameter
	case ref.URLParameter != "":
		return "url_parameter:" + ref.URLParameter
	}
	return ref.Field
}

func isPageDependent(ref properties.TransformationFieldReference) bool {
	if ref.URLParameter != "" {
		return true
	}
	info := loadCatalog().fields[ref.Field]
	return ref.Field != "" && info != nil && info.pageDependent()
}

func normalizeReference(ref *properties.TransformationFieldReference) (properties.TransformationFieldReference, error) {
	if ref == nil {
		return properties.TransformationFieldReference{}, fmt.Errorf("missing field reference")
	}
	result := *ref
	switch {
	case result.Parameter != "":
		if result.Field != "" || result.URLParameter != "" || (result.Type != "string" && result.Type != "number") ||
			utf8.RuneCountInString(result.Parameter) > MaxNameLength {
			return result, fmt.Errorf("parameter requires a name and string/number type, without field")
		}
		return result, nil
	case result.URLParameter != "":
		if result.Field != "" || (result.Type != "" && result.Type != "string") ||
			utf8.RuneCountInString(result.URLParameter) > MaxNameLength {
			return result, fmt.Errorf("url_parameter requires a name and no field or non-string type")
		}
		result.Type = ""
		return result, nil
	}
	result.Field = canonicalField(result.Field)
	if loadCatalog().fields[result.Field] == nil || result.Type != "" {
		return result, fmt.Errorf("unknown field reference")
	}
	return result, nil
}

func referenceType(ref properties.TransformationFieldReference) string {
	switch {
	case ref.Parameter != "":
		return ref.Type
	case ref.URLParameter != "":
		return "string"
	}
	if info := loadCatalog().fields[ref.Field]; info != nil {
		return info.capability.Type
	}
	return "string"
}

func writableReference(ref properties.TransformationFieldReference) (writable, removable bool) {
	if ref.Parameter != "" || ref.URLParameter != "" {
		return true, true
	}
	info := loadCatalog().fields[ref.Field]
	if info == nil {
		return false, false
	}
	return len(info.capability.WriteProtocols) > 0, info.capability.Removable
}

func compileAction(action properties.TransformationActionConfig, count int) (compiledAction, error) {
	result := compiledAction{kind: action.Type}
	switch action.Type {
	case "drop":
		if count != 1 || action.Target != nil || action.Value != nil {
			return result, fmt.Errorf("drop must be exclusive and have no target or value")
		}
		return result, nil
	case "copy":
		return result, fmt.Errorf("copy was replaced by set with a template value, e.g. {{page_title}}")
	case "set", "remove":
	default:
		return result, fmt.Errorf("unknown action type")
	}
	target, err := normalizeReference(action.Target)
	if err != nil {
		return result, err
	}
	writable, removable := writableReference(target)
	if !writable {
		return result, fmt.Errorf("target is read-only")
	}
	result.target = target
	if action.Type == "remove" {
		if !removable || action.Value != nil {
			return result, fmt.Errorf("remove requires a removable target and no value")
		}
		return result, nil
	}
	result.value, err = compileValue(action.Value, referenceType(target))
	return result, err
}

func compileValue(value *properties.TransformationLiteral, kind string) (compiledValue, error) {
	if value == nil {
		return compiledValue{}, fmt.Errorf("expected exactly one of string, number or template")
	}
	count := 0
	for _, present := range []bool{value.String != nil, value.Number != nil, value.Template != nil} {
		if present {
			count++
		}
	}
	if count != 1 {
		return compiledValue{}, fmt.Errorf("expected exactly one of string, number or template")
	}
	if value.Template != nil {
		return compileTemplate(*value.Template, kind)
	}
	if kind == "string" && value.String != nil && utf8.RuneCountInString(*value.String) <= MaxValueLength {
		return compiledValue{literal: *value.String}, nil
	}
	if kind == "number" && value.Number != nil && !math.IsNaN(*value.Number) && !math.IsInf(*value.Number, 0) {
		return compiledValue{literal: *value.Number}, nil
	}
	return compiledValue{}, fmt.Errorf(
		"literal has wrong type, is nonfinite or exceeds %d characters", MaxValueLength)
}

// compileTemplate parses "text {{field}} text". "\{{" is a literal "{{". References are
// catalog fields, params.<name>, numeric_params.<name> or url_params.<name>.
func compileTemplate(template, kind string) (compiledValue, error) {
	if utf8.RuneCountInString(template) > MaxValueLength {
		return compiledValue{}, fmt.Errorf("template exceeds %d characters", MaxValueLength)
	}
	var parts []templatePart
	var text strings.Builder
	for rest := template; rest != ""; {
		switch {
		case strings.HasPrefix(rest, `\{{`):
			text.WriteString("{{")
			rest = rest[3:]
		case strings.HasPrefix(rest, "{{"):
			end := strings.Index(rest, "}}")
			if end < 0 {
				return compiledValue{}, fmt.Errorf("template has an unclosed {{")
			}
			ref, err := templateReference(strings.TrimSpace(rest[2:end]))
			if err != nil {
				return compiledValue{}, err
			}
			if text.Len() > 0 {
				parts = append(parts, templatePart{text: text.String()})
				text.Reset()
			}
			parts = append(parts, templatePart{ref: &ref})
			rest = rest[end+2:]
		default:
			_, size := utf8.DecodeRuneInString(rest)
			text.WriteString(rest[:size])
			rest = rest[size:]
		}
	}
	if text.Len() > 0 {
		parts = append(parts, templatePart{text: text.String()})
	}
	if kind == "number" && (len(parts) != 1 || parts[0].ref == nil || referenceType(*parts[0].ref) != "number") {
		return compiledValue{}, fmt.Errorf("a number target needs a template with exactly one number field")
	}
	if len(parts) == 0 {
		parts = []templatePart{{text: ""}}
	}
	return compiledValue{template: parts}, nil
}

func templateReference(name string) (properties.TransformationFieldReference, error) {
	var ref properties.TransformationFieldReference
	switch {
	case strings.HasPrefix(name, paramsNamespace+"."):
		ref = properties.TransformationFieldReference{
			Parameter: strings.TrimPrefix(name, paramsNamespace+"."), Type: "string",
		}
	case strings.HasPrefix(name, numericParamsNamespace+"."):
		ref = properties.TransformationFieldReference{
			Parameter: strings.TrimPrefix(name, numericParamsNamespace+"."), Type: "number",
		}
	case strings.HasPrefix(name, urlParamsNamespace+"."):
		ref = properties.TransformationFieldReference{URLParameter: strings.TrimPrefix(name, urlParamsNamespace+".")}
	default:
		ref = properties.TransformationFieldReference{Field: name}
	}
	normalized, err := normalizeReference(&ref)
	if err != nil {
		return normalized, fmt.Errorf("template references an unknown field")
	}
	if normalized.Field != "" && loadCatalog().fields[normalized.Field].capability.ConditionOnly {
		return normalized, fmt.Errorf("template cannot reference %s", normalized.Field)
	}
	return normalized, nil
}
