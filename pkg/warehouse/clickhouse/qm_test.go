package clickhouse

import (
	"regexp"
	"strings"
	"testing"

	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/d8a-tech/d8a/pkg/warehouse/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// normalizeWhitespace removes all whitespace differences to compare SQL structure
func normalizeWhitespace(s string) string {
	// Replace all whitespace (spaces, tabs, newlines) with single spaces
	re := regexp.MustCompile(`\s+`)
	normalized := re.ReplaceAllString(s, " ")

	// Remove spaces around punctuation for consistent comparison
	punctuationPatterns := []struct {
		pattern     string
		replacement string
	}{
		{`\s*\(\s*`, "("}, // spaces around opening parentheses
		{`\s*\)\s*`, ")"}, // spaces around closing parentheses
		{`\s*,\s*`, ", "}, // normalize spaces around commas
	}

	for _, p := range punctuationPatterns {
		re := regexp.MustCompile(p.pattern)
		normalized = re.ReplaceAllString(normalized, p.replacement)
	}

	// Trim leading and trailing spaces
	return strings.TrimSpace(normalized)
}

func TestCreateQuery(t *testing.T) {
	// given

	testCases := []struct {
		name          string
		mapper        warehouse.QueryMapper
		expectedQuery string
	}{
		{
			name: "ClickHouse query mapper",
			mapper: NewClickHouseQueryMapper(
				WithEngine("MergeTree()"),
				WithPartitionBy("toYYYYMM(timestamp)"),
				WithOrderBy([]string{"event_type", "user_id", "timestamp"}),
				WithIndexGranularity(8192),
			),
			expectedQuery: ClickHouseCreateTableQuery,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			actualQuery, err := warehouse.CreateTableQuery(tc.mapper, "testdb.analytics_events", testutils.TestSchema())

			// then
			require.NoError(t, err)
			assert.Equal(t, normalizeWhitespace(tc.expectedQuery), normalizeWhitespace(actualQuery))
		})
	}
}

func TestQueryMapperArrowTypes(t *testing.T) {
	testutils.TestSupportedArrowTypes(t, NewClickHouseQueryMapper())
}

func TestQueryMapperTypeErrors(t *testing.T) {
	testutils.TestQueryMapperTypeErrors(t, NewClickHouseQueryMapper())
}
