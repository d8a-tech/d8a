package transformations

import (
	"regexp"
	"regexp/syntax"
	"strings"
	"unicode"
)

// replaceBranch is one alternative of a replace pattern. literals lists strings of which at
// least one must occur for the branch to match; nil means the branch always has to run.
type replaceBranch struct {
	pattern  *regexp.Regexp
	literals []string
}

// compileReplaceBranches splits a top-level alternation ("email|phone|...") into branches
// with literal prefilters, so values without "@" never run the email regex. Patterns with
// capture groups stay whole: splitting would renumber groups used by $1 replacements.
func compileReplaceBranches(pattern *regexp.Regexp) []replaceBranch {
	parsed, err := syntax.Parse(pattern.String(), syntax.Perl)
	if err != nil {
		return []replaceBranch{{pattern: pattern}}
	}
	if parsed.Op != syntax.OpAlternate || pattern.NumSubexp() > 0 {
		return []replaceBranch{{pattern: pattern, literals: requiredLiterals(parsed)}}
	}
	branches := make([]replaceBranch, 0, len(parsed.Sub))
	for _, sub := range parsed.Sub {
		compiled, err := regexp.Compile(sub.String())
		if err != nil {
			return []replaceBranch{{pattern: pattern, literals: requiredLiterals(parsed)}}
		}
		branches = append(branches, replaceBranch{pattern: compiled, literals: requiredLiterals(sub)})
	}
	return branches
}

// requiredLiterals returns strings of which at least one occurs in every match, or nil when
// no such guarantee can be derived cheaply.
func requiredLiterals(node *syntax.Regexp) []string {
	switch node.Op {
	case syntax.OpLiteral:
		text := string(node.Rune)
		if node.Flags&syntax.FoldCase != 0 && strings.IndexFunc(text, unicode.IsLetter) >= 0 {
			return nil
		}
		return []string{text}
	case syntax.OpCharClass:
		if len(node.Rune) == 2 && node.Rune[0] == node.Rune[1] {
			return []string{string(node.Rune[0])}
		}
	case syntax.OpCapture, syntax.OpPlus:
		return requiredLiterals(node.Sub[0])
	case syntax.OpRepeat:
		if node.Min >= 1 {
			return requiredLiterals(node.Sub[0])
		}
	case syntax.OpConcat:
		for _, sub := range node.Sub {
			if literals := requiredLiterals(sub); literals != nil {
				return literals
			}
		}
	case syntax.OpAlternate:
		var union []string
		for _, sub := range node.Sub {
			literals := requiredLiterals(sub)
			if literals == nil {
				return nil
			}
			union = append(union, literals...)
		}
		return union
	}
	return nil
}

func containsAny(text string, literals []string) bool {
	for _, literal := range literals {
		if strings.Contains(text, literal) {
			return true
		}
	}
	return false
}

// replace computes a replace action's new value from the pre-rule state. ok=false means
// nothing changed (missing value, no match or oversized input), so the write is skipped:
// no URL rebuild and no invalidation of memoized reads.
func (v *eventView) replace(action *compiledAction) (any, bool, error) {
	text, err := v.replaceInput(action)
	if err != nil || text == "" || len(text) > MaxReplaceInputLength {
		return nil, false, err
	}
	replaced := text
	for _, branch := range action.branches {
		if branch.literals != nil && !containsAny(replaced, branch.literals) {
			continue
		}
		replaced = branch.pattern.ReplaceAllString(replaced, action.replacement)
	}
	if replaced == text {
		return nil, false, nil
	}
	return replaced, true, nil
}

// replaceInput returns the text a replace action works on. Fields stored in a single
// request parameter are read raw, so rewriting them keeps everything the pattern doesn't
// match (for example URL parameters excluded from reports) and skips column evaluation.
func (v *eventView) replaceInput(action *compiledAction) (string, error) {
	if info := loadCatalog().fields[action.target.Field]; action.target.Field != "" && info != nil {
		if spec, ok := info.writes[v.protocol]; ok && spec.kind == writeQuery {
			return v.input.Hit.Request.QueryParams.Get(spec.key), nil
		}
	}
	current, err := v.get(action.target)
	if err != nil {
		return "", err
	}
	text, _ := current.(string)
	return text, nil
}
