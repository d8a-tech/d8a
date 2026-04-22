package clickhouse

import (
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// Metadata keys for per-table ClickHouse DDL hints stored in Arrow schema metadata.
// These allow callers to override the driver-level ORDER BY and PARTITION BY defaults
// on a per-schema basis without changing the warehouse.Driver interface.
const (
	// MetaOrderBy is the Arrow schema metadata key for a comma-separated list of
	// ORDER BY columns for ClickHouse table creation.
	// Example value: "toDate(timestamp), session_id"
	MetaOrderBy = "d8a.warehouse.clickhouse.order_by"

	// MetaPartitionBy is the Arrow schema metadata key for the PARTITION BY expression
	// for ClickHouse table creation.
	// Example value: "toYYYYMM(timestamp)"
	MetaPartitionBy = "d8a.warehouse.clickhouse.partition_by"
)

// SetOrderBy returns a new *arrow.Schema with the ClickHouse ORDER BY hint set.
// The columns are joined with ", " in the DDL; pass them individually.
// Existing schema metadata is preserved.
func SetOrderBy(schema *arrow.Schema, columns []string) *arrow.Schema {
	return setMeta(schema, MetaOrderBy, strings.Join(columns, ","))
}

// SetPartitionBy returns a new *arrow.Schema with the ClickHouse PARTITION BY hint set.
// Existing schema metadata is preserved.
func SetPartitionBy(schema *arrow.Schema, expr string) *arrow.Schema {
	return setMeta(schema, MetaPartitionBy, expr)
}

// GetOrderBy reads the ORDER BY columns from Arrow schema metadata.
// Returns nil if the key is absent or the value is empty.
func GetOrderBy(schema *arrow.Schema) []string {
	val := schemaMetaValue(schema, MetaOrderBy)
	if val == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// GetPartitionBy reads the PARTITION BY expression from Arrow schema metadata.
// Returns empty string if the key is absent.
func GetPartitionBy(schema *arrow.Schema) string {
	return schemaMetaValue(schema, MetaPartitionBy)
}

// setMeta returns a new schema with the given metadata key set.
func setMeta(schema *arrow.Schema, key, value string) *arrow.Schema {
	existing := schema.Metadata()
	keys := existing.Keys()
	vals := existing.Values()

	// Replace existing key if present, otherwise append.
	for i, k := range keys {
		if k != key {
			continue
		}
		newVals := make([]string, len(vals))
		copy(newVals, vals)
		newVals[i] = value
		md := arrow.NewMetadata(keys, newVals)
		return arrow.NewSchema(schema.Fields(), &md)
	}

	newKeys := make([]string, len(keys)+1)
	copy(newKeys, keys)
	newKeys[len(keys)] = key

	newVals := make([]string, len(vals)+1)
	copy(newVals, vals)
	newVals[len(vals)] = value

	md := arrow.NewMetadata(newKeys, newVals)
	return arrow.NewSchema(schema.Fields(), &md)
}

// schemaMetaValue returns the value for a metadata key from the schema, or "".
func schemaMetaValue(schema *arrow.Schema, key string) string {
	md := schema.Metadata()
	idx := md.FindKey(key)
	if idx < 0 {
		return ""
	}
	return md.Values()[idx]
}
