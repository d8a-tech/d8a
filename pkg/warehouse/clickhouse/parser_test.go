package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNestedType(t *testing.T) {
	// given
	testCases := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid nested type",
			input:    "Nested(field1 String, field2 Int32)",
			expected: true,
		},
		{
			name:     "valid nested type with complex fields",
			input:    "Nested(id UInt64, data Array(String))",
			expected: true,
		},
		{
			name:     "empty nested type",
			input:    "Nested()",
			expected: true,
		},
		{
			name:     "not nested - regular type",
			input:    "String",
			expected: false,
		},
		{
			name:     "not nested - array type",
			input:    "Array(String)",
			expected: false,
		},
		{
			name:     "not nested - partial match",
			input:    "Nested",
			expected: false,
		},
		{
			name:     "not nested - missing closing paren",
			input:    "Nested(field String",
			expected: false,
		},
		{
			name:     "not nested - empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "not nested - too short",
			input:    "Nest()",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result := isNestedType(tc.input)

			// then
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestClickhouseParser_ParseNestedFields(t *testing.T) {
	// given
	parser := &clickhouseParser{}
	testCases := []struct {
		name     string
		input    string
		expected []nestedField
	}{
		{
			name:  "simple nested fields",
			input: "Nested(field1 String, field2 Int32)",
			expected: []nestedField{
				{Name: "field1", TypeName: "String"},
				{Name: "field2", TypeName: "Int32"},
			},
		},
		{
			name:  "nested fields with complex types",
			input: "Nested(id UInt64, data Array(String), score Float64)",
			expected: []nestedField{
				{Name: "id", TypeName: "UInt64"},
				{Name: "data", TypeName: "Array(String)"},
				{Name: "score", TypeName: "Float64"},
			},
		},
		{
			name:  "nested fields with nested arrays",
			input: "Nested(tags Array(String), counts Array(UInt32))",
			expected: []nestedField{
				{Name: "tags", TypeName: "Array(String)"},
				{Name: "counts", TypeName: "Array(UInt32)"},
			},
		},
		{
			name:     "empty nested type",
			input:    "Nested()",
			expected: []nestedField{},
		},
		{
			name:     "not a nested type",
			input:    "String",
			expected: nil,
		},
		{
			name:     "single field",
			input:    "Nested(field1 String)",
			expected: []nestedField{{Name: "field1", TypeName: "String"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result := parser.parseNestedFields(tc.input)

			// then
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestClickhouseParser_Tokenize(t *testing.T) {
	// given
	parser := &clickhouseParser{}
	testCases := []struct {
		name        string
		input       string
		expected    []clickhouseToken
		expectError bool
	}{
		{
			name:  "simple field list",
			input: "field1 String, field2 Int32",
			expected: []clickhouseToken{
				{Type: "identifier", Value: "field1", Pos: 0},
				{Type: "identifier", Value: "String", Pos: 7},
				{Type: "comma", Value: ",", Pos: 13},
				{Type: "identifier", Value: "field2", Pos: 15},
				{Type: "identifier", Value: "Int32", Pos: 22},
			},
		},
		{
			name:  "field with array type",
			input: "data Array(String)",
			expected: []clickhouseToken{
				{Type: "identifier", Value: "data", Pos: 0},
				{Type: "identifier", Value: "Array", Pos: 5},
				{Type: "lparen", Value: "(", Pos: 10},
				{Type: "identifier", Value: "String", Pos: 11},
				{Type: "rparen", Value: ")", Pos: 17},
			},
		},
		{
			name:  "complex nested types",
			input: "id UInt64, data Array(String), score Float64",
			expected: []clickhouseToken{
				{Type: "identifier", Value: "id", Pos: 0},
				{Type: "identifier", Value: "UInt64", Pos: 3},
				{Type: "comma", Value: ",", Pos: 9},
				{Type: "identifier", Value: "data", Pos: 11},
				{Type: "identifier", Value: "Array", Pos: 16},
				{Type: "lparen", Value: "(", Pos: 21},
				{Type: "identifier", Value: "String", Pos: 22},
				{Type: "rparen", Value: ")", Pos: 28},
				{Type: "comma", Value: ",", Pos: 29},
				{Type: "identifier", Value: "score", Pos: 31},
				{Type: "identifier", Value: "Float64", Pos: 37},
			},
		},
		{
			name:     "empty input",
			input:    "",
			expected: []clickhouseToken{},
		},
		{
			name:     "whitespace only",
			input:    "   \t\n  ",
			expected: []clickhouseToken{},
		},
		{
			name:        "invalid character",
			input:       "field1 String@",
			expected:    nil,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result, err := parser.tokenize(tc.input)

			// then
			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestClickhouseParser_ParseFieldList(t *testing.T) {
	// given
	parser := &clickhouseParser{}
	testCases := []struct {
		name     string
		tokens   []clickhouseToken
		expected []nestedField
	}{
		{
			name: "simple field list",
			tokens: []clickhouseToken{
				{Type: "identifier", Value: "field1"},
				{Type: "identifier", Value: "String"},
				{Type: "comma", Value: ","},
				{Type: "identifier", Value: "field2"},
				{Type: "identifier", Value: "Int32"},
			},
			expected: []nestedField{
				{Name: "field1", TypeName: "String"},
				{Name: "field2", TypeName: "Int32"},
			},
		},
		{
			name: "field with array type",
			tokens: []clickhouseToken{
				{Type: "identifier", Value: "data"},
				{Type: "identifier", Value: "Array"},
				{Type: "lparen", Value: "("},
				{Type: "identifier", Value: "String"},
				{Type: "rparen", Value: ")"},
			},
			expected: []nestedField{
				{Name: "data", TypeName: "Array(String)"},
			},
		},
		{
			name: "single field without comma",
			tokens: []clickhouseToken{
				{Type: "identifier", Value: "field1"},
				{Type: "identifier", Value: "String"},
			},
			expected: []nestedField{
				{Name: "field1", TypeName: "String"},
			},
		},
		{
			name:     "empty token list",
			tokens:   []clickhouseToken{},
			expected: []nestedField{},
		},
		{
			name: "invalid - missing field name",
			tokens: []clickhouseToken{
				{Type: "comma", Value: ","},
				{Type: "identifier", Value: "String"},
			},
			expected: []nestedField{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result := parser.parseFieldList(tc.tokens)

			// then
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestClickhouseParser_ParseFieldType(t *testing.T) {
	// given
	parser := &clickhouseParser{}
	testCases := []struct {
		name         string
		tokens       []clickhouseToken
		startPos     int
		expectedType string
		expectedPos  int
	}{
		{
			name: "simple type",
			tokens: []clickhouseToken{
				{Type: "identifier", Value: "String"},
			},
			startPos:     0,
			expectedType: "String",
			expectedPos:  1,
		},
		{
			name: "array type",
			tokens: []clickhouseToken{
				{Type: "identifier", Value: "Array"},
				{Type: "lparen", Value: "("},
				{Type: "identifier", Value: "String"},
				{Type: "rparen", Value: ")"},
			},
			startPos:     0,
			expectedType: "Array(String)",
			expectedPos:  4,
		},
		{
			name: "nested array type",
			tokens: []clickhouseToken{
				{Type: "identifier", Value: "Array"},
				{Type: "lparen", Value: "("},
				{Type: "identifier", Value: "Array"},
				{Type: "lparen", Value: "("},
				{Type: "identifier", Value: "String"},
				{Type: "rparen", Value: ")"},
				{Type: "rparen", Value: ")"},
			},
			startPos:     0,
			expectedType: "Array(Array(String))",
			expectedPos:  7,
		},
		{
			name: "tuple type with multiple parameters",
			tokens: []clickhouseToken{
				{Type: "identifier", Value: "Tuple"},
				{Type: "lparen", Value: "("},
				{Type: "identifier", Value: "String"},
				{Type: "comma", Value: ","},
				{Type: "identifier", Value: "Int32"},
				{Type: "rparen", Value: ")"},
			},
			startPos:     0,
			expectedType: "Tuple(String, Int32)",
			expectedPos:  6,
		},
		{
			name:         "invalid - empty tokens",
			tokens:       []clickhouseToken{},
			startPos:     0,
			expectedType: "",
			expectedPos:  0,
		},
		{
			name: "invalid - non-identifier start",
			tokens: []clickhouseToken{
				{Type: "lparen", Value: "("},
			},
			startPos:     0,
			expectedType: "",
			expectedPos:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			resultType, resultPos := parser.parseFieldType(tc.tokens, tc.startPos)

			// then
			assert.Equal(t, tc.expectedType, resultType)
			assert.Equal(t, tc.expectedPos, resultPos)
		})
	}
}

func TestIsWhitespace(t *testing.T) {
	// given
	testCases := []struct {
		name     string
		input    byte
		expected bool
	}{
		{name: "space", input: ' ', expected: true},
		{name: "tab", input: '\t', expected: true},
		{name: "newline", input: '\n', expected: true},
		{name: "carriage return", input: '\r', expected: true},
		{name: "regular character", input: 'a', expected: false},
		{name: "digit", input: '5', expected: false},
		{name: "special character", input: '(', expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result := isWhitespace(tc.input)

			// then
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIsAlphaNumeric(t *testing.T) {
	// given
	testCases := []struct {
		name     string
		input    byte
		expected bool
	}{
		{name: "lowercase letter", input: 'a', expected: true},
		{name: "uppercase letter", input: 'Z', expected: true},
		{name: "digit", input: '5', expected: true},
		{name: "space", input: ' ', expected: false},
		{name: "underscore", input: '_', expected: false},
		{name: "special character", input: '(', expected: false},
		{name: "hyphen", input: '-', expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result := isAlphaNumeric(tc.input)

			// then
			assert.Equal(t, tc.expected, result)
		})
	}
}
