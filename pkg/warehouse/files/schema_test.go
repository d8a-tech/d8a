package files

import (
	"testing"
	"text/template"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaFingerprint_Returns16CharHash(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	fp := schemaFingerprint(schema)
	assert.Equal(t, 16, len(fp))
}

func TestEscapeTableName(t *testing.T) {
	assert.Equal(t, "events_2026-02-25", escapeTableName("events_2026-02-25"))
	assert.Equal(t, "______etc_passwd", escapeTableName("../../etc/passwd"))
}

func TestMarshalSchemaRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		schema *arrow.Schema
	}{
		{
			name: "primitives and timestamp",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{
						Name:     "id",
						Type:     arrow.PrimitiveTypes.Int64,
						Nullable: false,
						Metadata: arrow.NewMetadata([]string{"k1"}, []string{"v1"}),
					},
					{
						Name:     "name",
						Type:     arrow.BinaryTypes.String,
						Nullable: true,
					},
					{
						Name:     "score",
						Type:     arrow.PrimitiveTypes.Float64,
						Nullable: true,
					},
					{
						Name:     "active",
						Type:     arrow.FixedWidthTypes.Boolean,
						Nullable: true,
					},
					{
						Name:     "created_date",
						Type:     arrow.FixedWidthTypes.Date32,
						Nullable: true,
					},
					{
						Name:     "ts",
						Type:     &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"},
						Nullable: true,
					},
				},
				func() *arrow.Metadata {
					m := arrow.NewMetadata([]string{"schema_key"}, []string{"schema_value"})
					return &m
				}(),
			),
		},
		{
			name: "nested struct and list",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{
						Name: "attrs",
						Type: arrow.StructOf(
							arrow.Field{
								Name:     "ok",
								Type:     arrow.FixedWidthTypes.Boolean,
								Nullable: true,
								Metadata: arrow.NewMetadata([]string{"inner_k"}, []string{"inner_v"}),
							},
							arrow.Field{
								Name: "items",
								Type: arrow.ListOfField(arrow.Field{
									Name:     "item",
									Type:     arrow.BinaryTypes.String,
									Nullable: true,
								}),
								Nullable: true,
							},
						),
						Nullable: true,
					},
				},
				nil,
			),
		},
		{
			name: "map type",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{
						Name:     "data",
						Type:     arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String),
						Nullable: true,
						Metadata: arrow.NewMetadata([]string{"map_meta"}, []string{"map_value"}),
					},
				},
				nil,
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			schema := tt.schema
			originalFp := schema.Fingerprint()

			// when
			data, err := marshalSchema(schema)
			require.NoError(t, err)

			roundTripped, err := unmarshalSchema(data)
			require.NoError(t, err)

			// then
			assert.True(t, schema.Equal(roundTripped), "schemas should be equal")
			assert.Equal(t, originalFp, roundTripped.Fingerprint(), "fingerprints should match")
		})
	}
}

func TestSegmentRemoteKey(t *testing.T) {
	tmpl, err := template.New("path").Parse(
		"table={{.Table}}/schema={{.Schema}}/dt={{.Year}}/{{.MonthPadded}}/{{.DayPadded}}/" +
			"{{.SegmentID}}.{{.Extension}}",
	)
	require.NoError(t, err)

	key, err := segmentRemoteKey(
		tmpl,
		"events",
		"abc123",
		"seg-1",
		"csv",
		time.Date(2026, time.February, 25, 10, 2, 3, 0, time.UTC),
	)
	require.NoError(t, err)

	assert.Equal(t, "table=events/schema=abc123/dt=2026/02/25/seg-1.csv", key)
}
