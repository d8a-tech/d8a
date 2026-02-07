package clickhouse

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/warehouse/meta"
)

func isArrayType(typeStr string) bool {
	return len(typeStr) > 6 && typeStr[:6] == "Array(" && typeStr[len(typeStr)-1] == ')'
}

func extractArrayElementType(typeStr string) string {
	// Extract from Array(ElementType)
	return typeStr[6 : len(typeStr)-1]
}

func isLowCardinalityType(typeStr string) bool {
	return len(typeStr) > 15 && typeStr[:15] == "LowCardinality(" && typeStr[len(typeStr)-1] == ')'
}

func extractLowCardinalityInnerType(typeStr string) string {
	// Extract from LowCardinality(InnerType)
	return typeStr[15 : len(typeStr)-1]
}

// hasLowCardinalityMetadata checks if metadata contains clickhouse.low_cardinality=true (exactly "true")
func hasLowCardinalityMetadata(metadata arrow.Metadata) bool {
	if metadata.Len() == 0 {
		return false
	}
	value, found := metadata.GetValue(meta.ClickhouseLowCardinalityMetadata)
	return found && value == "true"
}
