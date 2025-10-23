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
		if docs.DisplayName == "" {
			result += fmt.Sprintf("%s: %s\n", docs.ColumnName, docs.Description)
		} else {
			result += fmt.Sprintf("%s (%s): %s\n", docs.DisplayName, docs.ColumnName, docs.Description)
		}
	}
	result += "Session-scoped event columns:\n"
	for _, col := range columns.SessionScopedEvent {
		if col.Docs().DisplayName == "" {
			result += fmt.Sprintf("%s: %s\n", col.Docs().ColumnName, col.Docs().Description)
		} else {
			result += fmt.Sprintf("%s (%s): %s\n", col.Docs().DisplayName, col.Docs().ColumnName, col.Docs().Description)
		}
	}
	result += "Session columns:\n"
	for _, col := range columns.Session {
		if col.Docs().DisplayName == "" {
			result += fmt.Sprintf("%s: %s\n", col.Docs().ColumnName, col.Docs().Description)
		} else {
			result += fmt.Sprintf("%s (%s): %s\n", col.Docs().DisplayName, col.Docs().ColumnName, col.Docs().Description)
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
		columnsJSON = append(columnsJSON, map[string]any{
			"scope":        "event",
			"name":         docs.ColumnName,
			"display_name": docs.DisplayName,
			"description":  docs.Description,
			"interface_id": docs.InterfaceID,
		})
	}
	for _, col := range columns.SessionScopedEvent {
		docs := col.Docs()
		columnsJSON = append(columnsJSON, map[string]any{
			"scope":        "session-scoped-event",
			"name":         docs.ColumnName,
			"display_name": docs.DisplayName,
			"description":  docs.Description,
			"interface_id": docs.InterfaceID,
		})
	}
	for _, col := range columns.Session {
		docs := col.Docs()
		columnsJSON = append(columnsJSON, map[string]any{
			"scope":        "session",
			"name":         docs.ColumnName,
			"display_name": docs.DisplayName,
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
	if err := writer.Write([]string{"scope", "interface_id", "name", "display_name", "description"}); err != nil {
		return "", fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write event columns
	for _, col := range columns.Event {
		docs := col.Docs()
		if err := writer.Write([]string{
			"event", docs.InterfaceID, docs.ColumnName, docs.DisplayName, docs.Description,
		}); err != nil {
			return "", fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	// Write session-scoped event columns
	for _, col := range columns.SessionScopedEvent {
		docs := col.Docs()
		row := []string{
			"session-scoped-event", docs.InterfaceID, docs.ColumnName, docs.DisplayName, docs.Description,
		}
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	// Write session columns
	for _, col := range columns.Session {
		docs := col.Docs()
		if err := writer.Write([]string{
			"session", docs.InterfaceID, docs.ColumnName, docs.DisplayName, docs.Description,
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
