package clickhouse

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/d8a-tech/d8a/pkg/warehouse/meta"
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

	// Build column definition starting with type
	columnDef := fieldType.TypeAsString

	// Append column modifiers (e.g., "DEFAULT") if present
	if fieldType.ColumnModifiers != "" && fieldType.DefaultSQLExpression != "" {
		columnDef = fmt.Sprintf("%s DEFAULT %s", columnDef, fieldType.DefaultSQLExpression)
	}

	// Append CODEC clause if metadata is present
	codecValue, hasCodec := warehouse.GetArrowMetadataValue(field.Metadata, meta.ClickhouseCodecMetadata)
	if hasCodec {
		codecValue = strings.TrimSpace(codecValue)
		if codecValue == "" {
			return "", fmt.Errorf("codec metadata value cannot be empty for field %s", field.Name)
		}
		// Basic validation: should start with CODEC(
		if !strings.HasPrefix(codecValue, "CODEC(") {
			return "", fmt.Errorf("invalid codec metadata value for field %s: must start with 'CODEC('", field.Name)
		}
		columnDef = fmt.Sprintf("%s %s", columnDef, codecValue)
	}

	return columnDef, nil
}
