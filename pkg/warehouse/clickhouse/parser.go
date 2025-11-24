package clickhouse

import (
	"fmt"
	"strings"
)

// nestedField represents a field in a ClickHouse Nested type
type nestedField struct {
	Name     string
	TypeName string
}

func isNestedType(typeStr string) bool {
	return len(typeStr) > 7 && typeStr[:7] == "Nested(" && typeStr[len(typeStr)-1] == ')'
}

// clickhouseParser handles parsing of ClickHouse type strings for field definitions
type clickhouseParser struct{}

// parseNestedFields parses nested field definitions from a Nested(...) type string
func (p *clickhouseParser) parseNestedFields(typeStr string) []nestedField {
	if !isNestedType(typeStr) {
		return nil
	}

	// Extract the inner content from Nested(...)
	inner := typeStr[7 : len(typeStr)-1] // Remove "Nested(" and ")"

	tokens, err := p.tokenize(inner)
	if err != nil {
		return []nestedField{} // Return empty slice instead of nil
	}

	return p.parseFieldList(tokens)
}

// tokenize converts a field list string into tokens
func (p *clickhouseParser) tokenize(input string) ([]clickhouseToken, error) {
	tokens := make([]clickhouseToken, 0)
	i := 0

	for i < len(input) {
		ch := input[i]

		// Skip whitespace
		if isWhitespace(ch) {
			i++
			continue
		}

		switch ch {
		case '(':
			tokens = append(tokens, clickhouseToken{Type: "lparen", Value: "(", Pos: i})
			i++
		case ')':
			tokens = append(tokens, clickhouseToken{Type: "rparen", Value: ")", Pos: i})
			i++
		case ',':
			tokens = append(tokens, clickhouseToken{Type: "comma", Value: ",", Pos: i})
			i++
		default:
			// Parse identifier
			start := i
			for i < len(input) && (isAlphaNumeric(input[i]) || input[i] == '_') {
				i++
			}

			if i == start {
				return nil, fmt.Errorf("unexpected character '%c' at position %d", ch, i)
			}

			identifier := input[start:i]
			tokens = append(tokens, clickhouseToken{Type: "identifier", Value: identifier, Pos: start})
		}
	}

	return tokens, nil
}

// parseFieldList parses a list of field definitions from tokens
func (p *clickhouseParser) parseFieldList(tokens []clickhouseToken) []nestedField {
	fields := make([]nestedField, 0)
	i := 0

	for i < len(tokens) {
		// Expect field name (identifier)
		if i >= len(tokens) || tokens[i].Type != "identifier" {
			break
		}
		fieldName := tokens[i].Value
		i++

		// Parse field type - this can be complex with nested parentheses
		fieldType, newPos := p.parseFieldType(tokens, i)
		if fieldType == "" {
			break
		}
		i = newPos

		fields = append(fields, nestedField{
			Name:     fieldName,
			TypeName: fieldType,
		})

		// Skip comma if present
		if i < len(tokens) && tokens[i].Type == "comma" {
			i++
		}
	}

	return fields
}

// parseFieldType parses a field type from tokens, handling nested parentheses
// Returns the type string and the new position in the token stream
func (p *clickhouseParser) parseFieldType(tokens []clickhouseToken, startPos int) (typeStr string, newPos int) {
	if startPos >= len(tokens) || tokens[startPos].Type != "identifier" {
		return "", startPos
	}

	typeName := tokens[startPos].Value
	pos := startPos + 1

	// Check if this type has parameters (parentheses)
	if pos < len(tokens) && tokens[pos].Type == "lparen" {
		// Find matching closing parenthesis
		parenCount := 1
		start := pos
		pos++ // Skip opening paren

		for pos < len(tokens) && parenCount > 0 {
			switch tokens[pos].Type {
			case "lparen":
				parenCount++
			case "rparen":
				parenCount--
			}
			pos++
		}

		if parenCount == 0 {
			// Reconstruct the full type including parameters
			var typeBuilder strings.Builder
			typeBuilder.WriteString(typeName)
			for i := start; i < pos; i++ {
				typeBuilder.WriteString(tokens[i].Value)
				// Add space after comma for readability
				if tokens[i].Type == "comma" && i+1 < pos {
					typeBuilder.WriteString(" ")
				}
			}
			return typeBuilder.String(), pos
		}
	}

	return typeName, pos
}

// clickhouseToken represents a token in ClickHouse type parsing
type clickhouseToken struct {
	Type  string // "identifier", "lparen", "rparen", "comma"
	Value string
	Pos   int
}

// Helper functions for character classification
func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isAlphaNumeric(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
}
