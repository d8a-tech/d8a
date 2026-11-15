package clickhouse

import (
	"regexp"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/d8a-tech/d8a/pkg/warehouse/meta"
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

func TestQueryMapperColumnName(t *testing.T) {
	mapper := NewClickHouseQueryMapper()

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple column name",
			input:    "user_id",
			expected: "`user_id`",
		},
		{
			name:     "column with space",
			input:    "user id",
			expected: "`user id`",
		},
		{
			name:     "SQL keyword",
			input:    "select",
			expected: "`select`",
		},
		{
			name:     "column with backtick",
			input:    "col`name",
			expected: "`col``name`",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result := mapper.ColumnName(tc.input)

			// then
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestQueryMapperTablePredicate(t *testing.T) {
	mapper := NewClickHouseQueryMapper()

	// given
	table := "`mydb`.`events`"

	// when
	result := mapper.TablePredicate(table)

	// then — TablePredicate should pass through the already-quoted name
	assert.Equal(t, "TABLE `mydb`.`events`", result)
}

func TestCreateQueryWithQuotedIdentifiers(t *testing.T) {
	// given
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
	}, nil)
	quotedTable := quoteFullTableName("testdb", "events")
	testCases := []struct {
		name          string
		opts          []Options
		expectedQuery string
	}{
		{
			name: "default cluster",
			expectedQuery: "CREATE TABLE `testdb`.`events` (\n  `id` String\n) " +
				"ENGINE = MergeTree()\nORDER BY (id);",
		},
		{
			name: "empty cluster",
			opts: []Options{WithCluster("")},
			expectedQuery: "CREATE TABLE `testdb`.`events` (\n  `id` String\n) " +
				"ENGINE = MergeTree()\nORDER BY (id);",
		},
		{
			name: "named cluster",
			opts: []Options{WithCluster("analytics_cluster")},
			expectedQuery: "CREATE TABLE IF NOT EXISTS `testdb`.`events` ON CLUSTER `analytics_cluster` " +
				"(\n  `id` String\n) ENGINE = MergeTree()\nORDER BY (id);",
		},
		{
			name: "cluster with backtick",
			opts: []Options{WithCluster("analytics`cluster")},
			expectedQuery: "CREATE TABLE IF NOT EXISTS `testdb`.`events` ON CLUSTER `analytics``cluster` " +
				"(\n  `id` String\n) ENGINE = MergeTree()\nORDER BY (id);",
		},
		{
			name: "cluster with replicated engine",
			opts: []Options{
				WithCluster("analytics_cluster"),
				WithEngine("ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')"),
			},
			expectedQuery: "CREATE TABLE IF NOT EXISTS `testdb`.`events` ON CLUSTER `analytics_cluster` " +
				"(\n  `id` String\n) " +
				"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')\nORDER BY (id);",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			opts := append([]Options{WithOrderBy([]string{"id"})}, tc.opts...)
			mapper := NewClickHouseQueryMapper(opts...)

			// when
			query, err := warehouse.CreateTableQuery(mapper, quotedTable, schema)

			// then
			require.NoError(t, err)
			assert.Equal(t, tc.expectedQuery, query)
		})
	}
}

func TestQueryMapperTypeErrors(t *testing.T) {
	testutils.TestQueryMapperTypeErrors(t, NewClickHouseQueryMapper())
}

func TestQueryMapperCodec(t *testing.T) {
	// given
	mapper := NewClickHouseQueryMapper()

	testCases := []struct {
		name          string
		field         *arrow.Field
		expected      string
		expectedError bool
	}{
		{
			name: "field with codec metadata",
			field: &arrow.Field{
				Name:     "test_column",
				Type:     arrow.BinaryTypes.String,
				Nullable: false,
				Metadata: arrow.NewMetadata(
					[]string{meta.ClickhouseCodecMetadata},
					[]string{meta.Codec("Delta", "ZSTD")},
				),
			},
			expected:      "String CODEC(Delta, ZSTD)",
			expectedError: false,
		},
		{
			name: "nullable field with codec and DEFAULT",
			field: &arrow.Field{
				Name:     "test_column",
				Type:     arrow.BinaryTypes.String,
				Nullable: true,
				Metadata: arrow.NewMetadata(
					[]string{meta.ClickhouseCodecMetadata},
					[]string{meta.Codec("Delta", "ZSTD")},
				),
			},
			expected:      "String DEFAULT '' CODEC(Delta, ZSTD)",
			expectedError: false,
		},
		{
			name: "field with codec without compression",
			field: &arrow.Field{
				Name:     "test_column",
				Type:     arrow.PrimitiveTypes.Int64,
				Nullable: false,
				Metadata: arrow.NewMetadata(
					[]string{meta.ClickhouseCodecMetadata},
					[]string{meta.Codec("Delta", "")},
				),
			},
			expected:      "Int64 CODEC(Delta)",
			expectedError: false,
		},
		{
			name: "field with empty codec metadata value",
			field: &arrow.Field{
				Name:     "test_column",
				Type:     arrow.BinaryTypes.String,
				Nullable: false,
				Metadata: arrow.NewMetadata(
					[]string{meta.ClickhouseCodecMetadata},
					[]string{""},
				),
			},
			expected:      "",
			expectedError: true,
		},
		{
			name: "field with invalid codec metadata value",
			field: &arrow.Field{
				Name:     "test_column",
				Type:     arrow.BinaryTypes.String,
				Nullable: false,
				Metadata: arrow.NewMetadata(
					[]string{meta.ClickhouseCodecMetadata},
					[]string{"INVALID_CODEC"},
				),
			},
			expected:      "",
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// when
			result, err := mapper.Field(tc.field)

			// then
			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
