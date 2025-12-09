package cmd

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/d8a-tech/d8a/pkg/schema"
)

// arrowTypeToUserFriendly converts Arrow type names to user-friendly type names
func arrowTypeToUserFriendly(arrowTypeName string) string {
	result := arrowTypeName

	// Replace Arrow types with user-friendly equivalents
	replacements := map[string]string{
		"utf8":                  "String",
		"int64":                 "Numeric (Int64)",
		"int32":                 "Numeric (Int32)",
		"int16":                 "Numeric (Int16)",
		"int8":                  "Numeric (Int8)",
		"uint64":                "Numeric (UInt64)",
		"uint32":                "Numeric (UInt32)",
		"uint16":                "Numeric (UInt16)",
		"uint8":                 "Numeric (UInt8)",
		"float64":               "Numeric (Float64)",
		"float32":               "Numeric (Float32)",
		"double":                "Numeric (Float64)",
		"bool":                  "Boolean",
		"date32":                "Date",
		"date64":                "Date",
		"timestamp[s, tz=UTC]":  "Datetime (s, UTC)",
		"timestamp[ms, tz=UTC]": "Datetime (ms, UTC)",
		"timestamp[us, tz=UTC]": "Datetime (us, UTC)",
		"timestamp[ns, tz=UTC]": "Datetime (ns, UTC)",
		"timestamp[s]":          "Datetime (s)",
		"timestamp[ms]":         "Datetime (ms)",
		"timestamp[us]":         "Datetime (us)",
		"timestamp[ns]":         "Datetime (ns)",
		"struct<":               "Object<",
		"list<":                 "Array<",
	}

	for old, new := range replacements {
		result = strings.ReplaceAll(result, old, new)
	}

	return result
}

type columnsFormatter interface {
	Format(columns schema.Columns) (string, error)
}

type consoleColumnsFormatter struct {
}

func (f *consoleColumnsFormatter) Format(columns schema.Columns) (string, error) {
	result := ""
	result += "Event columns:\n"
	for _, col := range columns.Event {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = arrowTypeToUserFriendly(docs.Type.Type.String())
		}
		result += fmt.Sprintf("%s [%s]: %s\n", docs.ColumnName, typeName, docs.Description)
	}
	result += "Session-scoped event columns:\n"
	for _, col := range columns.SessionScopedEvent {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = arrowTypeToUserFriendly(docs.Type.Type.String())
		}
		result += fmt.Sprintf("%s [%s]: %s\n", docs.ColumnName, typeName, docs.Description)
	}
	result += "Session columns:\n"
	for _, col := range columns.Session {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = arrowTypeToUserFriendly(docs.Type.Type.String())
		}
		result += fmt.Sprintf("%s [%s]: %s\n", docs.ColumnName, typeName, docs.Description)
	}
	return result, nil
}

func newConsoleColumnsFormatter() columnsFormatter {
	return &consoleColumnsFormatter{}
}

type jsonColumnsFormatter struct {
}

func (f *jsonColumnsFormatter) Format(columns schema.Columns) (string, error) {
	columnsJSON := []map[string]any{}
	for _, col := range columns.Event {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = arrowTypeToUserFriendly(docs.Type.Type.String())
		}
		columnsJSON = append(columnsJSON, map[string]any{
			"name":         docs.ColumnName,
			"display_name": docs.DisplayName,
			"scope":        "event",
			"type":         typeName,
			"description":  docs.Description,
			"interface_id": docs.InterfaceID,
		})
	}
	for _, col := range columns.SessionScopedEvent {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = arrowTypeToUserFriendly(docs.Type.Type.String())
		}
		columnsJSON = append(columnsJSON, map[string]any{
			"name":         docs.ColumnName,
			"display_name": docs.DisplayName,
			"scope":        "session-scoped-event",
			"type":         typeName,
			"description":  docs.Description,
			"interface_id": docs.InterfaceID,
		})
	}
	for _, col := range columns.Session {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = arrowTypeToUserFriendly(docs.Type.Type.String())
		}
		columnsJSON = append(columnsJSON, map[string]any{
			"name":         docs.ColumnName,
			"display_name": docs.DisplayName,
			"scope":        "session",
			"type":         typeName,
			"description":  docs.Description,
			"interface_id": docs.InterfaceID,
		})
	}
	jsonBytes, err := json.MarshalIndent(columnsJSON, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal columns to JSON: %w", err)
	}
	return string(jsonBytes), nil
}

func newJSONColumnsFormatter() columnsFormatter {
	return &jsonColumnsFormatter{}
}

type csvColumnsFormatter struct {
}

func (f *csvColumnsFormatter) Format(columns schema.Columns) (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Write header
	if err := writer.Write([]string{"name", "display_name", "scope", "type", "description", "interface_id"}); err != nil {
		return "", fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write event columns
	for _, col := range columns.Event {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = arrowTypeToUserFriendly(docs.Type.Type.String())
		}
		if err := writer.Write([]string{
			docs.ColumnName, docs.DisplayName, "event", typeName, docs.Description, docs.InterfaceID,
		}); err != nil {
			return "", fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	// Write session-scoped event columns
	for _, col := range columns.SessionScopedEvent {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = arrowTypeToUserFriendly(docs.Type.Type.String())
		}
		row := []string{
			docs.ColumnName, docs.DisplayName, "session-scoped-event", typeName, docs.Description, docs.InterfaceID,
		}
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	// Write session columns
	for _, col := range columns.Session {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = arrowTypeToUserFriendly(docs.Type.Type.String())
		}
		if err := writer.Write([]string{
			docs.ColumnName, docs.DisplayName, "session", typeName, docs.Description, docs.InterfaceID,
		}); err != nil {
			return "", fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	return buf.String(), nil
}

func newCSVColumnsFormatter() columnsFormatter {
	return &csvColumnsFormatter{}
}

type markdownColumnsFormatter struct {
}

// nolint // this is a documentation generator, does not need to meet production code quality standards
func (f *markdownColumnsFormatter) Format(columns schema.Columns) (string, error) {
	var buf bytes.Buffer

	// Write table header
	buf.WriteString("| Name | Display Name | Scope | Type | Description |\n")
	buf.WriteString("|------|--------------|-------|------|-------------|\n")

	// Write event columns
	for _, col := range columns.Event {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		typeDisplay := ""
		if typeName != "" {
			friendlyType := arrowTypeToUserFriendly(typeName)
			typeDisplay = fmt.Sprintf("`%s`", escapeMarkdownType(friendlyType))
		}
		description := docs.Description
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s |\n",
			escapeMarkdownCell(docs.ColumnName),
			escapeMarkdownCell(docs.DisplayName),
			"`event`",
			typeDisplay,
			escapeMarkdownCell(description)))
	}

	// Write session-scoped event columns
	for _, col := range columns.SessionScopedEvent {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		typeDisplay := ""
		if typeName != "" {
			friendlyType := arrowTypeToUserFriendly(typeName)
			typeDisplay = fmt.Sprintf("`%s`", escapeMarkdownType(friendlyType))
		}
		description := docs.Description
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s |\n",
			escapeMarkdownCell(docs.ColumnName),
			escapeMarkdownCell(docs.DisplayName),
			"`session-scoped-event`",
			typeDisplay,
			escapeMarkdownCell(description)))
	}

	// Write session columns
	for _, col := range columns.Session {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		typeDisplay := ""
		if typeName != "" {
			friendlyType := arrowTypeToUserFriendly(typeName)
			typeDisplay = fmt.Sprintf("`%s`", escapeMarkdownType(friendlyType))
		}
		description := docs.Description
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s |\n",
			escapeMarkdownCell(docs.ColumnName),
			escapeMarkdownCell(docs.DisplayName),
			"`session`",
			typeDisplay,
			escapeMarkdownCell(description)))
	}

	return buf.String(), nil
}

func escapeMarkdownType(text string) string {
	// Escape types while keeping angle brackets (for generic types like List<T>)
	result := ""
	for _, r := range text {
		switch r {
		case '|':
			result += "\\|"
		case '\n':
			result += " "
		case '\r':
			// Skip carriage return
		default:
			result += string(r)
		}
	}
	return result
}

func escapeMarkdownCell(text string) string {
	// Replace pipe characters, newlines, and angle brackets that could break the table or Docusaurus rendering
	result := ""
	for _, r := range text {
		switch r {
		case '|':
			result += "\\|"
		case '<', '>':
			// Remove angle brackets for Docusaurus compatibility
			continue
		case '\n':
			result += " "
		case '\r':
			// Skip carriage return
		default:
			result += string(r)
		}
	}
	return result
}

func newMarkdownColumnsFormatter() columnsFormatter {
	return &markdownColumnsFormatter{}
}
