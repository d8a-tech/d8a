package clickhouse

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetGetOrderBy(t *testing.T) {
	// given
	base := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// when
	schema := SetOrderBy(base, []string{"col_a", "col_b"})
	result := GetOrderBy(schema)

	// then
	assert.Equal(t, []string{"col_a", "col_b"}, result)
}

func TestSetGetPartitionBy(t *testing.T) {
	// given
	base := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// when
	schema := SetPartitionBy(base, "toYYYYMM(ts)")
	result := GetPartitionBy(schema)

	// then
	assert.Equal(t, "toYYYYMM(ts)", result)
}

func TestGetOrderBy_absent(t *testing.T) {
	// given
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// when / then
	assert.Nil(t, GetOrderBy(schema))
}

func TestGetPartitionBy_absent(t *testing.T) {
	// given
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// when / then
	assert.Empty(t, GetPartitionBy(schema))
}

func TestCreateTableQuery_schemaMetaOverridesOrderBy(t *testing.T) {
	// given: driver mapper with default order_by
	mapper := newClickHouseQueryMapper(
		WithOrderBy([]string{"default_col"}),
		WithPartitionBy("default_partition"),
	)

	base := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	schema := SetOrderBy(base, []string{"col_a", "col_b"})

	// when
	overrideMapper := mapper.withHints(GetOrderBy(schema), GetPartitionBy(schema))
	query, err := warehouse.CreateTableQuery(overrideMapper, "db.tbl", schema)

	// then
	require.NoError(t, err)
	assert.Contains(t, query, "ORDER BY (col_a, col_b)")
	// partition_by falls back to empty (not set in schema)
	assert.NotContains(t, query, "PARTITION BY")
}

func TestCreateTableQuery_schemaMetaOverridesPartitionBy(t *testing.T) {
	// given
	mapper := newClickHouseQueryMapper(
		WithOrderBy([]string{"default_col"}),
		WithPartitionBy("default_partition"),
	)

	base := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	schema := SetPartitionBy(base, "toYYYYMM(ts)")

	// when: only partition_by is overridden; order_by keeps driver default
	overrideMapper := mapper.withHints(mapper.orderBy, GetPartitionBy(schema))
	query, err := warehouse.CreateTableQuery(overrideMapper, "db.tbl", schema)

	// then
	require.NoError(t, err)
	assert.Contains(t, query, "PARTITION BY toYYYYMM(ts)")
	assert.Contains(t, query, "ORDER BY (default_col)")
}

func TestCreateTableQuery_defaultsApplyWhenMetaAbsent(t *testing.T) {
	// given
	mapper := newClickHouseQueryMapper(
		WithOrderBy([]string{"default_col"}),
		WithPartitionBy("default_partition"),
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// when: no metadata → use mapper directly (no withHints)
	query, err := warehouse.CreateTableQuery(mapper, "db.tbl", schema)

	// then
	require.NoError(t, err)
	assert.Contains(t, query, "ORDER BY (default_col)")
	assert.Contains(t, query, "PARTITION BY default_partition")
}

func TestCreateTableQuery_multipleOrderByColumns(t *testing.T) {
	// given
	mapper := newClickHouseQueryMapper()

	base := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	schema := SetOrderBy(base, []string{"toDate(ts)", "session_id", "event_id"})

	// when
	overrideMapper := mapper.withHints(GetOrderBy(schema), GetPartitionBy(schema))
	query, err := warehouse.CreateTableQuery(overrideMapper, "db.tbl", schema)

	// then
	require.NoError(t, err)
	assert.Contains(t, query, "ORDER BY (toDate(ts), session_id, event_id)")
}
