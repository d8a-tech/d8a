package files

import (
	"bytes"
	"encoding/csv"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

// TestNewCSVFormat_AcceptsOptions verifies format constructor accepts options.
func TestNewCSVFormat_AcceptsOptions(t *testing.T) {
	// given: format with compression option
	format := NewCSVFormat(WithCompression("gzip"))

	// then: format is created without error
	assert.NotNil(t, format)
	assert.Equal(t, "csv", format.Extension())
}

// TestCSVFormat_Write_WithVariousTypes verifies CSV writer handles different Arrow types.
func TestCSVFormat_Write_WithVariousTypes(t *testing.T) {
	tests := []struct {
		name           string
		schema         *arrow.Schema
		rows           []map[string]any
		expectedHeader string
		expectedRows   []string
	}{
		{
			name: "scalar types",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "name", Type: arrow.BinaryTypes.String},
					{Name: "score", Type: arrow.PrimitiveTypes.Float64},
					{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
				},
				nil,
			),
			rows: []map[string]any{
				{"id": int64(1), "name": "Alice", "score": 95.5, "active": true},
				{"id": int64(2), "name": "Bob", "score": 87.3, "active": false},
			},
			expectedHeader: "id,name,score,active",
			expectedRows: []string{
				"1,Alice,95.5,true",
				"2,Bob,87.3,false",
			},
		},
		{
			name: "timestamp type",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "event_time", Type: arrow.FixedWidthTypes.Timestamp_us},
					{Name: "event", Type: arrow.BinaryTypes.String},
				},
				nil,
			),
			rows: []map[string]any{
				{"event_time": time.Date(2026, 2, 24, 14, 30, 0, 0, time.UTC), "event": "click"},
				{"event_time": time.Date(2026, 2, 24, 14, 31, 0, 0, time.UTC), "event": "view"},
			},
			expectedHeader: "event_time,event",
			expectedRows: []string{
				"2026-02-24T14:30:00Z,click",
				"2026-02-24T14:31:00Z,view",
			},
		},
		{
			name: "null values",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "optional_name", Type: arrow.BinaryTypes.String},
				},
				nil,
			),
			rows: []map[string]any{
				{"id": int64(1), "optional_name": "Alice"},
				{"id": int64(2), "optional_name": nil},
			},
			expectedHeader: "id,optional_name",
			expectedRows: []string{
				"1,Alice",
				"2,",
			},
		},
		{
			name: "complex types JSON encoded",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
					{Name: "metadata", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
				},
				nil,
			),
			rows: []map[string]any{
				{
					"id":       int64(1),
					"tags":     []string{"important", "urgent"},
					"metadata": map[string]string{"source": "web", "version": "1.0"},
				},
			},
			expectedHeader: "id,tags,metadata",
			expectedRows: []string{
				`1,"[""important"",""urgent""]","{""source"":""web"",""version"":""1.0""}"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given: CSV format and buffer
			format := NewCSVFormat()
			var buf bytes.Buffer

			// when: writing rows
			err := format.Write(&buf, tt.schema, tt.rows)

			// then: no error and correct output
			assert.NoError(t, err)

			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			assert.Equal(t, len(tt.rows)+1, len(lines), "should have header + data rows")
			assert.Equal(t, tt.expectedHeader, lines[0], "header should match")

			for i, expectedRow := range tt.expectedRows {
				assert.Equal(t, expectedRow, lines[i+1], "row %d should match", i)
			}
		})
	}
}

// TestCSVFormat_Write_RFC4180Quoting verifies proper quoting for special characters.
func TestCSVFormat_Write_RFC4180Quoting(t *testing.T) {
	tests := []struct {
		name         string
		value        string
		expectedCell string
	}{
		{
			name:         "value with comma",
			value:        "Hello, World",
			expectedCell: "Hello, World", // CSV reader removes quotes
		},
		{
			name:         "value with quote",
			value:        `Say "Hi"`,
			expectedCell: `Say "Hi"`,
		},
		{
			name:         "value with newline",
			value:        "Line1\nLine2",
			expectedCell: "Line1\nLine2",
		},
		{
			name:         "simple value no quotes",
			value:        "simple",
			expectedCell: "simple",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given: schema with string field
			schema := arrow.NewSchema(
				[]arrow.Field{
					{Name: "text", Type: arrow.BinaryTypes.String},
				},
				nil,
			)
			rows := []map[string]any{
				{"text": tt.value},
			}

			// when: writing to CSV
			format := NewCSVFormat()
			var buf bytes.Buffer
			err := format.Write(&buf, schema, rows)

			// then: proper quoting applied and value preserved
			assert.NoError(t, err)

			// Parse CSV properly (handles quoted fields with newlines)
			reader := csv.NewReader(&buf)
			records, err := reader.ReadAll()
			assert.NoError(t, err)
			assert.Equal(t, 2, len(records), "should have header + 1 data row")
			assert.Equal(t, "text", records[0][0], "header should be 'text'")
			assert.Equal(t, tt.expectedCell, records[1][0], "cell value should match")
		})
	}
}

// TestCSVFormat_Write_AppendBehavior verifies header only written once on append.
func TestCSVFormat_Write_AppendBehavior(t *testing.T) {
	// given: schema and data
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	batch1 := []map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}

	batch2 := []map[string]any{
		{"id": int64(3), "name": "Charlie"},
		{"id": int64(4), "name": "Diana"},
	}

	// when: writing first batch to new file
	tmpDir := t.TempDir()
	filePath := tmpDir + "/test.csv"

	file1, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o644)
	assert.NoError(t, err)

	format := NewCSVFormat()
	err = format.Write(file1, schema, batch1)
	assert.NoError(t, err)
	assert.NoError(t, file1.Close())

	// when: appending second batch
	file2, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0o644)
	assert.NoError(t, err)

	err = format.Write(file2, schema, batch2)
	assert.NoError(t, err)
	assert.NoError(t, file2.Close())

	// then: file contains header once and all rows
	content, err := os.ReadFile(filePath)
	assert.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	assert.Equal(t, 5, len(lines), "should have 1 header + 4 data rows")
	assert.Equal(t, "id,name", lines[0], "header should be first line")
	assert.Equal(t, "1,Alice", lines[1])
	assert.Equal(t, "2,Bob", lines[2])
	assert.Equal(t, "3,Charlie", lines[3])
	assert.Equal(t, "4,Diana", lines[4])
}

// TestCSVFormat_Write_EmptyRows verifies handling of empty row slice.
func TestCSVFormat_Write_EmptyRows(t *testing.T) {
	// given: schema but no rows
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	rows := []map[string]any{}

	// when: writing empty rows
	format := NewCSVFormat()
	var buf bytes.Buffer
	err := format.Write(&buf, schema, rows)

	// then: only header written
	assert.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Equal(t, 1, len(lines), "should have only header")
	assert.Equal(t, "id,name", lines[0])
}

// TestCSVFormat_Write_NonSeekableWriter verifies behavior with non-seekable writers.
func TestCSVFormat_Write_NonSeekableWriter(t *testing.T) {
	// given: non-seekable buffer (regular buffer doesn't implement io.Seeker)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	rows := []map[string]any{
		{"id": int64(1)},
	}

	// when: writing to non-seekable writer
	format := NewCSVFormat()
	var buf bytes.Buffer
	err := format.Write(&buf, schema, rows)

	// then: header is written (safe default for non-seekable)
	assert.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Equal(t, 2, len(lines))
	assert.Equal(t, "id", lines[0])
	assert.Equal(t, "1", lines[1])
}

// TestValueToString_Primitives verifies conversion of primitive types.
func TestValueToString_Primitives(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{name: "nil", value: nil, expected: ""},
		{name: "string", value: "hello", expected: "hello"},
		{name: "bool true", value: true, expected: "true"},
		{name: "bool false", value: false, expected: "false"},
		{name: "int64", value: int64(123), expected: "123"},
		{name: "int32", value: int32(456), expected: "456"},
		{name: "int", value: 789, expected: "789"},
		{name: "float64", value: 123.45, expected: "123.45"},
		{name: "float32", value: float32(67.89), expected: "67.89"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when: converting value
			result, err := valueToString(tt.value, arrow.BinaryTypes.String)

			// then: correct string representation
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestValueToString_Timestamp verifies RFC3339 formatting for timestamps.
func TestValueToString_Timestamp(t *testing.T) {
	// given: timestamp value
	timestamp := time.Date(2026, 2, 24, 14, 30, 45, 123456789, time.UTC)

	// when: converting to string
	result, err := valueToString(timestamp, arrow.FixedWidthTypes.Timestamp_us)

	// then: RFC3339 format used
	assert.NoError(t, err)
	assert.Equal(t, "2026-02-24T14:30:45Z", result)
}

// TestValueToString_ComplexTypes verifies JSON encoding for complex types.
func TestValueToString_ComplexTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{
			name:     "string slice",
			value:    []string{"a", "b", "c"},
			expected: `["a","b","c"]`,
		},
		{
			name:     "map",
			value:    map[string]string{"key": "value"},
			expected: `{"key":"value"}`,
		},
		{
			name:     "nested structure",
			value:    map[string]any{"items": []int{1, 2, 3}},
			expected: `{"items":[1,2,3]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when: converting complex value
			result, err := valueToString(tt.value, arrow.BinaryTypes.String)

			// then: JSON encoded
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCSVFormat_Write_Integration verifies full write cycle with real CSV parsing.
func TestCSVFormat_Write_Integration(t *testing.T) {
	// given: schema and rows
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64},
			{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
		},
		nil,
	)

	rows := []map[string]any{
		{
			"id":        int64(1),
			"name":      "Alice",
			"score":     95.5,
			"timestamp": time.Date(2026, 2, 24, 14, 30, 0, 0, time.UTC),
		},
		{
			"id":        int64(2),
			"name":      "Bob",
			"score":     87.3,
			"timestamp": time.Date(2026, 2, 24, 14, 31, 0, 0, time.UTC),
		},
	}

	// when: writing to CSV
	format := NewCSVFormat()
	var buf bytes.Buffer
	err := format.Write(&buf, schema, rows)
	assert.NoError(t, err)

	// then: output is valid CSV that can be parsed
	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(records), "should have header + 2 rows")

	// Verify header
	assert.Equal(t, []string{"id", "name", "score", "timestamp"}, records[0])

	// Verify first row
	assert.Equal(t, "1", records[1][0])
	assert.Equal(t, "Alice", records[1][1])
	assert.Equal(t, "95.5", records[1][2])
	assert.Equal(t, "2026-02-24T14:30:00Z", records[1][3])

	// Verify second row
	assert.Equal(t, "2", records[2][0])
	assert.Equal(t, "Bob", records[2][1])
	assert.Equal(t, "87.3", records[2][2])
	assert.Equal(t, "2026-02-24T14:31:00Z", records[2][3])
}
