package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteIdentifier(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "events",
			expected: "`events`",
		},
		{
			name:     "identifier with space",
			input:    "my table",
			expected: "`my table`",
		},
		{
			name:     "identifier with backtick",
			input:    "col`name",
			expected: "`col``name`",
		},
		{
			name:     "identifier with multiple backticks",
			input:    "a`b`c",
			expected: "`a``b``c`",
		},
		{
			name:     "SQL keyword",
			input:    "select",
			expected: "`select`",
		},
		{
			name:     "identifier with special characters",
			input:    "col-name.with/chars",
			expected: "`col-name.with/chars`",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "``",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result := quoteIdentifier(tc.input)

			// then
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestQuoteFullTableName(t *testing.T) {
	testCases := []struct {
		name     string
		database string
		table    string
		expected string
	}{
		{
			name:     "simple names",
			database: "testdb",
			table:    "events",
			expected: "`testdb`.`events`",
		},
		{
			name:     "database with space",
			database: "my database",
			table:    "events",
			expected: "`my database`.`events`",
		},
		{
			name:     "table with backtick",
			database: "db",
			table:    "my`table",
			expected: "`db`.`my``table`",
		},
		{
			name:     "both with special chars",
			database: "db`1",
			table:    "table name",
			expected: "`db``1`.`table name`",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result := quoteFullTableName(tc.database, tc.table)

			// then
			assert.Equal(t, tc.expected, result)
		})
	}
}
