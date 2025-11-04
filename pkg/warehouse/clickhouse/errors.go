package clickhouse

import "strings"

func isAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "already exists") ||
		strings.Contains(errStr, "Already exists") ||
		strings.Contains(errStr, "code: 57")
}
