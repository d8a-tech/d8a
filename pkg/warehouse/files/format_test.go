package files

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCSVFormat_Write_WithVariousTypes verifies CSV writer handles different Arrow types.
func TestCSVFormat_Write_WithVariousTypes(t *testing.T) {
	tests := []struct {
		name           string
		schema         *arrow.Schema
		rows           []map[string]any
		expectedHeader string
		expectedRows   []string
		useCSVReader   bool
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
			useCSVReader: false,
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
				{"event_time": time.Date(2026, 2, 24, 14, 30, 45, 123456789, time.UTC), "event": "click"},
				{"event_time": time.Date(2026, 2, 24, 14, 31, 0, 0, time.UTC), "event": "view"},
			},
			expectedHeader: "event_time,event",
			expectedRows: []string{
				"2026-02-24T14:30:45.123456789Z,click",
				"2026-02-24T14:31:00Z,view",
			},
			useCSVReader: true,
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
			useCSVReader: false,
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
			useCSVReader: false,
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

			if tt.useCSVReader {
				reader := csv.NewReader(&buf)
				records, err := reader.ReadAll()
				assert.NoError(t, err)
				assert.Equal(t, len(tt.rows)+1, len(records), "should have header + data rows")
				assert.Equal(t, strings.Split(tt.expectedHeader, ","), records[0], "header should match")

				for i, expectedRow := range tt.expectedRows {
					assert.Equal(t, strings.Split(expectedRow, ","), records[i+1], "row %d should match", i)
				}
				return
			}

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
	assert.Equal(t, "2026-02-24T14:30:45.123456789Z", result)
}

// TestValueToString_ComplexTypes verifies JSON encoding for complex types.
func TestValueToString_ComplexTypes(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		assertFn func(t *testing.T, result string)
	}{
		{
			name:  "string slice",
			value: []string{"a", "b", "c"},
			assertFn: func(t *testing.T, result string) {
				assert.Equal(t, `["a","b","c"]`, result)
			},
		},
		{
			name:  "map",
			value: map[string]string{"key": "value"},
			assertFn: func(t *testing.T, result string) {
				var decoded map[string]any
				require.NoError(t, json.Unmarshal([]byte(result), &decoded))
				assert.Equal(t, "value", decoded["key"])
			},
		},
		{
			name:  "nested structure",
			value: map[string]any{"items": []int{1, 2, 3}},
			assertFn: func(t *testing.T, result string) {
				var decoded map[string]any
				require.NoError(t, json.Unmarshal([]byte(result), &decoded))
				items, ok := decoded["items"].([]any)
				require.True(t, ok)
				assert.Equal(t, []any{float64(1), float64(2), float64(3)}, items)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when: converting complex value
			result, err := valueToString(tt.value, arrow.BinaryTypes.String)

			// then: JSON encoded
			assert.NoError(t, err)
			tt.assertFn(t, result)
		})
	}
}

// TestCSVFormat_Gzip_Extension verifies the extension changes with gzip compression.
func TestCSVFormat_Gzip_Extension(t *testing.T) {
	// given / when / then
	assert.Equal(t, "csv", NewCSVFormat().Extension(), "no compression → csv")
	assert.Equal(t, "csv.gz", NewCSVFormat(WithCompression(Gzip(gzip.DefaultCompression))).Extension(), "gzip → csv.gz")
}

// TestCSVFormat_Gzip_Write_RoundTrip verifies gzip-compressed CSV can be read back.
func TestCSVFormat_Gzip_Write_RoundTrip(t *testing.T) {
	// given
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	rows := []map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	format := NewCSVFormat(WithCompression(Gzip(gzip.DefaultCompression)))

	// when
	var buf bytes.Buffer
	err := format.Write(&buf, schema, rows)
	require.NoError(t, err)

	// then: decompress and parse
	gr, err := gzip.NewReader(&buf)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, gr.Close())
	}()

	reader := csv.NewReader(gr)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Equal(t, []string{"id", "name"}, records[0], "header row")
	assert.Equal(t, []string{"1", "Alice"}, records[1])
	assert.Equal(t, []string{"2", "Bob"}, records[2])
}

// TestCSVFormat_Gzip_AppendBehavior verifies multi-stream gzip append: header written once.
func TestCSVFormat_Gzip_AppendBehavior(t *testing.T) {
	// given
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
	format := NewCSVFormat(WithCompression(Gzip(gzip.BestSpeed)))
	tmpDir := t.TempDir()
	filePath := tmpDir + "/test.csv.gz"

	// when: write first batch to new file
	file1, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	require.NoError(t, format.Write(file1, schema, batch1))
	require.NoError(t, file1.Close())

	// when: append second batch
	file2, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	require.NoError(t, format.Write(file2, schema, batch2))
	require.NoError(t, file2.Close())

	// then: read all gzip members and collect all CSV records
	rawFile, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rawFile.Close())
	}()

	var allRecords [][]string
	gr, err := gzip.NewReader(rawFile)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, gr.Close())
	}()
	for {
		csvData, readErr := io.ReadAll(gr)
		require.NoError(t, readErr)
		r := csv.NewReader(bytes.NewReader(csvData))
		recs, parseErr := r.ReadAll()
		require.NoError(t, parseErr)
		allRecords = append(allRecords, recs...)
		if err = gr.Reset(rawFile); err != nil {
			break // no more gzip members
		}
	}

	// then: exactly one header row and all 4 data rows
	assert.Equal(t, 5, len(allRecords), "1 header + 4 data rows")
	assert.Equal(t, []string{"id", "name"}, allRecords[0], "header")
	assert.Equal(t, []string{"1", "Alice"}, allRecords[1])
	assert.Equal(t, []string{"2", "Bob"}, allRecords[2])
	assert.Equal(t, []string{"3", "Charlie"}, allRecords[3])
	assert.Equal(t, []string{"4", "Diana"}, allRecords[4])
}
