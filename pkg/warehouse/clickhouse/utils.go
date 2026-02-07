package clickhouse

func isArrayType(typeStr string) bool {
	return len(typeStr) > 6 && typeStr[:6] == "Array(" && typeStr[len(typeStr)-1] == ')'
}

func extractArrayElementType(typeStr string) string {
	// Extract from Array(ElementType)
	return typeStr[6 : len(typeStr)-1]
}
