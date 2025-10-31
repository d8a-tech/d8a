package cmd

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/schema"
)

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
			typeName = docs.Type.Type.String()
		}
		if docs.DisplayName == "" {
			result += fmt.Sprintf("%s [%s]: %s\n", docs.ColumnName, typeName, docs.Description)
		} else {
			result += fmt.Sprintf("%s (%s) [%s]: %s\n", docs.DisplayName, docs.ColumnName, typeName, docs.Description)
		}
	}
	result += "Session-scoped event columns:\n"
	for _, col := range columns.SessionScopedEvent {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		if docs.DisplayName == "" {
			result += fmt.Sprintf("%s [%s]: %s\n", docs.ColumnName, typeName, docs.Description)
		} else {
			result += fmt.Sprintf("%s (%s) [%s]: %s\n", docs.DisplayName, docs.ColumnName, typeName, docs.Description)
		}
	}
	result += "Session columns:\n"
	for _, col := range columns.Session {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		if docs.DisplayName == "" {
			result += fmt.Sprintf("%s [%s]: %s\n", docs.ColumnName, typeName, docs.Description)
		} else {
			result += fmt.Sprintf("%s (%s) [%s]: %s\n", docs.DisplayName, docs.ColumnName, typeName, docs.Description)
		}
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
			typeName = docs.Type.Type.String()
		}
		columnsJSON = append(columnsJSON, map[string]any{
			"scope":        "event",
			"name":         docs.ColumnName,
			"display_name": docs.DisplayName,
			"description":  docs.Description,
			"interface_id": docs.InterfaceID,
			"type":         typeName,
		})
	}
	for _, col := range columns.SessionScopedEvent {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		columnsJSON = append(columnsJSON, map[string]any{
			"scope":        "session-scoped-event",
			"name":         docs.ColumnName,
			"display_name": docs.DisplayName,
			"description":  docs.Description,
			"interface_id": docs.InterfaceID,
			"type":         typeName,
		})
	}
	for _, col := range columns.Session {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		columnsJSON = append(columnsJSON, map[string]any{
			"scope":        "session",
			"name":         docs.ColumnName,
			"display_name": docs.DisplayName,
			"description":  docs.Description,
			"interface_id": docs.InterfaceID,
			"type":         typeName,
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
	if err := writer.Write([]string{"scope", "interface_id", "name", "display_name", "type", "description"}); err != nil {
		return "", fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write event columns
	for _, col := range columns.Event {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		if err := writer.Write([]string{
			"event", docs.InterfaceID, docs.ColumnName, docs.DisplayName, typeName, docs.Description,
		}); err != nil {
			return "", fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	// Write session-scoped event columns
	for _, col := range columns.SessionScopedEvent {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		row := []string{
			"session-scoped-event", docs.InterfaceID, docs.ColumnName, docs.DisplayName, typeName, docs.Description,
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
			typeName = docs.Type.Type.String()
		}
		if err := writer.Write([]string{
			"session", docs.InterfaceID, docs.ColumnName, docs.DisplayName, typeName, docs.Description,
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
	buf.WriteString("| Name | Display Name | Type | Description |\n")
	buf.WriteString("|------|--------------|------|-------------|\n")

	// Write event columns
	for _, col := range columns.Event {
		docs := col.Docs()
		typeName := ""
		if docs.Type != nil {
			typeName = docs.Type.Type.String()
		}
		typeDisplay := ""
		if typeName != "" {
			typeDisplay = fmt.Sprintf("`%s`", escapeMarkdownType(typeName))
		}
		description := docs.Description
		if description != "" {
			description += fmt.Sprintf(" Column scope: `event`, Column interface: `%s`.", docs.InterfaceID)
		} else {
			description = fmt.Sprintf("Column scope: `event`, Column interface:`%s`.", docs.InterfaceID)
		}
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
			escapeMarkdownCell(docs.ColumnName),
			escapeMarkdownCell(docs.DisplayName),
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
			typeDisplay = fmt.Sprintf("`%s`", escapeMarkdownType(typeName))
		}
		description := docs.Description
		if description != "" {
			description += fmt.Sprintf(" Column scope: `session-scoped-event`, Column interface: `%s`.", docs.InterfaceID)
		} else {
			description = fmt.Sprintf("Column scope: `session-scoped-event`, Column interface: `%s`.", docs.InterfaceID)
		}
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
			escapeMarkdownCell(docs.ColumnName),
			escapeMarkdownCell(docs.DisplayName),
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
			typeDisplay = fmt.Sprintf("`%s`", escapeMarkdownType(typeName))
		}
		description := docs.Description
		if description != "" {
			description += fmt.Sprintf(" Column scope: `session`, Column interface: `%s`.", docs.InterfaceID)
		} else {
			description = fmt.Sprintf("Column scope: `session`, Column interface: `%s`.", docs.InterfaceID)
		}
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
			escapeMarkdownCell(docs.ColumnName),
			escapeMarkdownCell(docs.DisplayName),
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
