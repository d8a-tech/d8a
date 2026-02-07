package meta

// ClickhouseLowCardinalityMetadata is the metadata key used to indicate
// that a field should be stored as LowCardinality in ClickHouse
const ClickhouseLowCardinalityMetadata = "d8a.warehouse.clickhouse.low_cardinality"

// ColumnDescriptionMetadataKey is the metadata key used to store column descriptions
// in Arrow field metadata. This is used to pass descriptions from schema definitions
// to warehouse drivers (e.g., BigQuery) that support column descriptions.
const ColumnDescriptionMetadataKey = "d8a.column.description"
