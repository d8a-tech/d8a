package meta

import "fmt"

// ClickhouseLowCardinalityMetadata is the metadata key used to indicate
// that a field should be stored as LowCardinality in ClickHouse
const ClickhouseLowCardinalityMetadata = "d8a.warehouse.clickhouse.low_cardinality"

// ColumnDescriptionMetadataKey is the metadata key used to store column descriptions
// in Arrow field metadata. This is used to pass descriptions from schema definitions
// to warehouse drivers (e.g., BigQuery) that support column descriptions.
const ColumnDescriptionMetadataKey = "d8a.column.description"

// ClickhouseCodecMetadata is the metadata key used to indicate
// that a field should have a CODEC clause in ClickHouse DDL
const ClickhouseCodecMetadata = "d8a.warehouse.clickhouse.codec"

// Codec returns a CODEC clause string for ClickHouse DDL.
// If compressionAlg is empty, returns "CODEC(codec)".
// If both are provided, returns "CODEC(codec, compressionAlg)".
func Codec(codec, compressionAlg string) string {
	if compressionAlg == "" {
		return fmt.Sprintf("CODEC(%s)", codec)
	}
	return fmt.Sprintf("CODEC(%s, %s)", codec, compressionAlg)
}
