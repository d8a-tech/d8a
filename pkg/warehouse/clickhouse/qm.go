package clickhouse

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
)

type clickhouseQueryMapper struct {
	fieldTypeMapper  warehouse.FieldTypeMapper[SpecificClickhouseType]
	engine           string
	partitionBy      string
	orderBy          []string
	indexGranularity int
}

// NewClickHouseQueryMapper creates a new ClickHouse query mapper.
func NewClickHouseQueryMapper(opts ...Options) warehouse.QueryMapper {
	q := &clickhouseQueryMapper{
		engine:          "MergeTree()",
		fieldTypeMapper: NewFieldTypeMapper(),
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Options represents a configuration option for ClickHouse query mapper.
type Options func(*clickhouseQueryMapper)

// WithEngine sets the engine type for ClickHouse tables.
func WithEngine(engine string) Options {
	return func(q *clickhouseQueryMapper) {
		q.engine = engine
	}
}

// WithPartitionBy sets the partition expression for ClickHouse tables.
func WithPartitionBy(partitionBy string) Options {
	return func(q *clickhouseQueryMapper) {
		q.partitionBy = partitionBy
	}
}

// WithOrderBy sets the order by columns for ClickHouse tables.
func WithOrderBy(orderBy []string) Options {
	return func(q *clickhouseQueryMapper) {
		q.orderBy = orderBy
	}
}

// WithIndexGranularity sets the index granularity for ClickHouse tables.
func WithIndexGranularity(granularity int) Options {
	return func(q *clickhouseQueryMapper) {
		q.indexGranularity = granularity
	}
}

func (q *clickhouseQueryMapper) TablePredicate(table string) string {
	return fmt.Sprintf("TABLE %s", table)
}

func (q *clickhouseQueryMapper) TableSuffix(_ string) string {
	var parts []string

	parts = append(parts, fmt.Sprintf("ENGINE = %s", q.engine))

	if q.partitionBy != "" {
		parts = append(parts, fmt.Sprintf("PARTITION BY %s", q.partitionBy))
	}

	if len(q.orderBy) > 0 {
		parts = append(parts, fmt.Sprintf("ORDER BY (%s)", strings.Join(q.orderBy, ", ")))
	}
	if q.indexGranularity > 0 {
		parts = append(parts, fmt.Sprintf("SETTINGS index_granularity = %d", q.indexGranularity))
	}

	return strings.Join(parts, "\n") + ";"
}

func (q *clickhouseQueryMapper) Field(field *arrow.Field) (string, error) {
	fieldType, err := q.fieldTypeMapper.ArrowToWarehouse(warehouse.ArrowType{
		ArrowDataType: field.Type,
		Nullable:      field.Nullable,
		Metadata:      field.Metadata,
	})
	if err != nil {
		return "", err
	}
	// Append column modifiers (e.g., "DEFAULT") if present
	if fieldType.ColumnModifiers != "" {
		// For DEFAULT modifier, we need to provide explicit default values based on type
		defaultExpr := q.getDefaultExpression(field.Type)
		return fmt.Sprintf("%s DEFAULT %s", fieldType.TypeAsString, defaultExpr), nil
	}
	return fieldType.TypeAsString, nil
}

// getDefaultExpression returns the explicit default expression for a given Arrow type
// ClickHouse requires explicit default values when using DEFAULT modifier
func (q *clickhouseQueryMapper) getDefaultExpression(dataType arrow.DataType) string {
	switch dataType {
	case arrow.BinaryTypes.String:
		return "''"
	case arrow.FixedWidthTypes.Boolean:
		return "0"
	case arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64:
		return "0"
	case arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64:
		return "0"
	case arrow.FixedWidthTypes.Timestamp_s:
		return "'1970-01-01 00:00:00'"
	case arrow.FixedWidthTypes.Date32:
		return "'1970-01-01'"
	default:
		// Fallback for unknown types
		return "''"
	}
}
