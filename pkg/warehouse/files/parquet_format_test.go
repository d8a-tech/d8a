package files

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/parquet-go/parquet-go"
	parquetSnappy "github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetFormat_WriteRows_RoundTripScalarTypes(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ratio", Type: arrow.PrimitiveTypes.Float64},
		{Name: "enabled", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_s},
		{Name: "event_date", Type: arrow.FixedWidthTypes.Date32},
	}, nil)

	rows := []map[string]any{
		{
			"name":       "first",
			"count":      float64(10),
			"ratio":      float64(1.25),
			"enabled":    true,
			"created_at": "2026-02-24T14:30:45Z",
			"event_date": "2026-02-24",
		},
		{
			"name":       "second",
			"count":      int64(11),
			"ratio":      float64(2.5),
			"enabled":    false,
			"created_at": time.Date(2026, 2, 25, 12, 0, 0, 0, time.UTC),
			"event_date": time.Date(2026, 2, 25, 8, 0, 0, 0, time.FixedZone("utc+2", 2*3600)),
		},
	}

	var buf bytes.Buffer
	writer, err := NewParquetFormat().NewWriter(&buf, schema)
	require.NoError(t, err)
	require.NoError(t, writer.WriteRows(rows))
	require.NoError(t, writer.Close())

	actualRows := readParquetRows(t, buf.Bytes())
	require.Len(t, actualRows, 2)

	assert.Equal(t, "first", actualRows[0]["name"])
	assert.Equal(t, int64(10), actualRows[0]["count"])
	assert.Equal(t, float64(1.25), actualRows[0]["ratio"])
	assert.Equal(t, true, actualRows[0]["enabled"])
	assertTimestampMillis(t, actualRows[0]["created_at"], time.Date(2026, 2, 24, 14, 30, 45, 0, time.UTC))
	assertDate32Days(t, actualRows[0]["event_date"], time.Date(2026, 2, 24, 0, 0, 0, 0, time.UTC))

	assert.Equal(t, "second", actualRows[1]["name"])
	assert.Equal(t, int64(11), actualRows[1]["count"])
	assert.Equal(t, float64(2.5), actualRows[1]["ratio"])
	assert.Equal(t, false, actualRows[1]["enabled"])
	assertTimestampMillis(t, actualRows[1]["created_at"], time.Date(2026, 2, 25, 12, 0, 0, 0, time.UTC))
	assertDate32Days(t, actualRows[1]["event_date"], time.Date(2026, 2, 25, 0, 0, 0, 0, time.UTC))
}

func TestParquetFormat_WriteRows_RoundTripListStruct(t *testing.T) {
	itemType := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "ratio", Type: arrow.PrimitiveTypes.Float64},
	)
	listType := arrow.ListOfField(arrow.Field{Name: "element", Type: itemType, Nullable: false})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "items", Type: listType},
	}, nil)

	rows := []map[string]any{
		{
			"items": []any{
				map[string]any{"name": "a", "count": float64(1), "ratio": float64(1.5)},
				map[string]any{"name": "b", "count": float64(2), "ratio": float64(2.5)},
			},
		},
	}

	var buf bytes.Buffer
	writer, err := NewParquetFormat().NewWriter(&buf, schema)
	require.NoError(t, err)
	require.NoError(t, writer.WriteRows(rows))
	require.NoError(t, writer.Close())

	actualRows := readParquetRows(t, buf.Bytes())
	require.Len(t, actualRows, 1)

	parquetReader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
	t.Cleanup(func() {
		assert.NoError(t, parquetReader.Close())
	})

	parquetRows := make([]parquet.Row, 1)
	n, err := parquetReader.ReadRows(parquetRows)
	if err != nil {
		require.True(t, errors.Is(err, io.EOF))
	}
	require.Equal(t, 1, n)

	names := make([]string, 0, 2)
	counts := make([]int64, 0, 2)
	ratios := make([]float64, 0, 2)
	for _, value := range parquetRows[0] {
		if value.Kind() == parquet.ByteArray {
			names = append(names, string(value.ByteArray()))
		}
		if value.Kind() == parquet.Int64 {
			counts = append(counts, value.Int64())
		}
		if value.Kind() == parquet.Double {
			ratios = append(ratios, value.Double())
		}
	}

	assert.Equal(t, []string{"a", "b"}, names)
	assert.Equal(t, []int64{1, 2}, counts)
	assert.Equal(t, []float64{1.5, 2.5}, ratios)
}

func TestParquetFormat_WriteRows_NullableFieldRoundTrip(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "optional_name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	rows := []map[string]any{
		{"id": float64(1), "optional_name": "alice"},
		{"id": float64(2), "optional_name": nil},
	}

	var buf bytes.Buffer
	writer, err := NewParquetFormat().NewWriter(&buf, schema)
	require.NoError(t, err)
	require.NoError(t, writer.WriteRows(rows))
	require.NoError(t, writer.Close())

	actualRows := readParquetRows(t, buf.Bytes())
	require.Len(t, actualRows, 2)
	assert.Equal(t, "alice", actualRows[0]["optional_name"])
	assert.Nil(t, actualRows[1]["optional_name"])
}

func TestParquetFormatWriter_WriteRows_EmptyRowsAndMultipleBatches(t *testing.T) {
	t.Run("empty row set", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

		var buf bytes.Buffer
		writer, err := NewParquetFormat().NewWriter(&buf, schema)
		require.NoError(t, err)
		require.NoError(t, writer.WriteRows([]map[string]any{}))
		require.NoError(t, writer.Close())

		actualRows := readParquetRows(t, buf.Bytes())
		assert.Len(t, actualRows, 0)
	})

	t.Run("multiple WriteRows calls", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

		var buf bytes.Buffer
		writer, err := NewParquetFormat().NewWriter(&buf, schema)
		require.NoError(t, err)
		require.NoError(t, writer.WriteRows([]map[string]any{{"id": float64(1)}, {"id": float64(2)}}))
		require.NoError(t, writer.WriteRows([]map[string]any{{"id": float64(3)}}))
		require.NoError(t, writer.Close())

		actualRows := readParquetRows(t, buf.Bytes())
		require.Len(t, actualRows, 3)
		assert.Equal(t, int64(1), actualRows[0]["id"])
		assert.Equal(t, int64(2), actualRows[1]["id"])
		assert.Equal(t, int64(3), actualRows[2]["id"])
	})
}

func TestParquetFormat_WithCompression_WritesValidParquet(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rows := []map[string]any{{"id": float64(1)}, {"id": float64(2)}}

	var compressed bytes.Buffer
	compressedWriter, err := NewParquetFormat(
		WithParquetCompression(&parquetSnappy.Codec{}),
	).NewWriter(&compressed, schema)
	require.NoError(t, err)
	require.NoError(t, compressedWriter.WriteRows(rows))
	require.NoError(t, compressedWriter.Close())

	actualRows := readParquetRows(t, compressed.Bytes())
	require.Len(t, actualRows, 2)
	assert.Equal(t, int64(1), actualRows[0]["id"])
	assert.Equal(t, int64(2), actualRows[1]["id"])
}

func TestParquetFormatWriter_WriteAfterCloseReturnsError(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	var buf bytes.Buffer
	writer, err := NewParquetFormat().NewWriter(&buf, schema)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	err = writer.WriteRows([]map[string]any{{"id": float64(1)}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format writer is closed")

	assert.NoError(t, writer.Close())
}

func readParquetRows(t *testing.T, parquetBytes []byte) []map[string]any {
	t.Helper()

	fileReader := bytes.NewReader(parquetBytes)
	file, err := parquet.OpenFile(fileReader, int64(len(parquetBytes)))
	require.NoError(t, err)

	reader := parquet.NewGenericReader[map[string]any](
		bytes.NewReader(parquetBytes),
		&parquet.ReaderConfig{Schema: file.Schema()},
	)
	t.Cleanup(func() {
		assert.NoError(t, reader.Close())
	})

	rows := make([]map[string]any, 0)
	buffer := make([]map[string]any, 16)

	for {
		for i := range buffer {
			buffer[i] = make(map[string]any)
		}

		n, err := reader.Read(buffer)
		if n > 0 {
			for i := range n {
				rowCopy := make(map[string]any, len(buffer[i]))
				for key, value := range buffer[i] {
					rowCopy[key] = value
				}
				rows = append(rows, rowCopy)
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
	}

	return rows
}

func assertTimestampMillis(t *testing.T, value any, expected time.Time) {
	t.Helper()

	timestampMillis, ok := value.(int64)
	require.True(t, ok)

	assert.Equal(t, expected, time.UnixMilli(timestampMillis).UTC())
}

func assertDate32Days(t *testing.T, value any, expected time.Time) {
	t.Helper()

	days, ok := value.(int32)
	require.True(t, ok)

	assert.Equal(t, expected, time.Unix(int64(days)*24*3600, 0).UTC())
}
