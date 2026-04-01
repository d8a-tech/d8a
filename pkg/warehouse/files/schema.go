package files

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// schemaFingerprint returns a 16-character SHA256-based fingerprint for the schema.
func schemaFingerprint(schema *arrow.Schema) string {
	arrowFp := schema.Fingerprint()
	hash := sha256.Sum256([]byte(arrowFp))
	hexStr := fmt.Sprintf("%x", hash)
	if len(hexStr) > 16 {
		return hexStr[:16]
	}
	return hexStr
}

// escapeTableName replaces unsafe characters with underscores.
func escapeTableName(table string) string {
	if table == "" {
		return table
	}

	var builder strings.Builder
	builder.Grow(len(table))
	for _, ch := range table {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			builder.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		case ch == '-' || ch == '_':
			builder.WriteRune(ch)
		default:
			builder.WriteRune('_')
		}
	}

	return builder.String()
}

type metadataEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type schemaWire struct {
	Fields   []fieldWire     `json:"fields"`
	Metadata []metadataEntry `json:"metadata,omitempty"`
}

type fieldWire struct {
	Name     string          `json:"name"`
	Nullable bool            `json:"nullable"`
	Type     dataTypeWire    `json:"type"`
	Metadata []metadataEntry `json:"metadata,omitempty"`
}

type dataTypeWire struct {
	Kind      string      `json:"kind"`
	Unit      string      `json:"unit,omitempty"`
	Timezone  string      `json:"timezone,omitempty"`
	ElemField *fieldWire  `json:"elem_field,omitempty"`
	Fields    []fieldWire `json:"fields,omitempty"`
}

func marshalSchema(schema *arrow.Schema) ([]byte, error) {
	w, err := schemaToWire(schema)
	if err != nil {
		return nil, fmt.Errorf("converting schema to wire: %w", err)
	}

	data, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("marshaling schema JSON: %w", err)
	}

	return data, nil
}

func unmarshalSchema(data []byte) (*arrow.Schema, error) {
	var w schemaWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("unmarshaling schema JSON: %w", err)
	}

	schema, err := wireToSchema(w)
	if err != nil {
		return nil, fmt.Errorf("converting wire schema: %w", err)
	}

	return schema, nil
}

func schemaToWire(schema *arrow.Schema) (schemaWire, error) {
	fields := make([]fieldWire, 0, len(schema.Fields()))
	for _, f := range schema.Fields() {
		wf, err := fieldToWire(&f)
		if err != nil {
			return schemaWire{}, err
		}
		fields = append(fields, wf)
	}

	return schemaWire{Fields: fields, Metadata: metadataToEntries(schema.Metadata())}, nil
}

func wireToSchema(w schemaWire) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(w.Fields))
	for i := range w.Fields {
		f, err := wireToField(&w.Fields[i])
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	md := entriesToMetadata(w.Metadata)
	return arrow.NewSchema(fields, &md), nil
}

func fieldToWire(f *arrow.Field) (fieldWire, error) {
	dt, err := dataTypeToWire(f.Type)
	if err != nil {
		return fieldWire{}, fmt.Errorf("field %s: %w", f.Name, err)
	}

	return fieldWire{
		Name:     f.Name,
		Nullable: f.Nullable,
		Type:     dt,
		Metadata: metadataToEntries(f.Metadata),
	}, nil
}

func wireToField(w *fieldWire) (arrow.Field, error) {
	dt, err := wireToDataType(&w.Type)
	if err != nil {
		return arrow.Field{}, fmt.Errorf("field %s: %w", w.Name, err)
	}

	return arrow.Field{
		Name:     w.Name,
		Type:     dt,
		Nullable: w.Nullable,
		Metadata: entriesToMetadata(w.Metadata),
	}, nil
}

func dataTypeToWire(dt arrow.DataType) (dataTypeWire, error) {
	switch t := dt.(type) {
	case *arrow.StringType:
		return dataTypeWire{Kind: "string"}, nil
	case *arrow.Int64Type:
		return dataTypeWire{Kind: "int64"}, nil
	case *arrow.BooleanType:
		return dataTypeWire{Kind: "bool"}, nil
	case *arrow.Float64Type:
		return dataTypeWire{Kind: "float64"}, nil
	case *arrow.TimestampType:
		return dataTypeWire{Kind: "timestamp", Unit: t.Unit.String(), Timezone: t.TimeZone}, nil
	case *arrow.Date32Type:
		return dataTypeWire{Kind: "date32"}, nil
	case *arrow.ListType:
		elemField := t.ElemField()
		elem, err := fieldToWire(&elemField)
		if err != nil {
			return dataTypeWire{}, err
		}
		return dataTypeWire{Kind: "list", ElemField: &elem}, nil
	case *arrow.StructType:
		fields := make([]fieldWire, 0, t.NumFields())
		for _, f := range t.Fields() {
			wf, err := fieldToWire(&f)
			if err != nil {
				return dataTypeWire{}, err
			}
			fields = append(fields, wf)
		}
		return dataTypeWire{Kind: "struct", Fields: fields}, nil
	default:
		return dataTypeWire{}, fmt.Errorf("unsupported data type %T", dt)
	}
}

func wireToDataType(w *dataTypeWire) (arrow.DataType, error) {
	switch w.Kind {
	case "string":
		return arrow.BinaryTypes.String, nil
	case "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "timestamp":
		unit, err := parseTimestampUnit(w.Unit)
		if err != nil {
			return nil, err
		}
		return &arrow.TimestampType{Unit: unit, TimeZone: w.Timezone}, nil
	case "date32":
		return arrow.FixedWidthTypes.Date32, nil
	case "list":
		if w.ElemField == nil {
			return nil, fmt.Errorf("list missing elem field")
		}
		elemField, err := wireToField(w.ElemField)
		if err != nil {
			return nil, err
		}
		return arrow.ListOfField(elemField), nil
	case "struct":
		fields := make([]arrow.Field, 0, len(w.Fields))
		for i := range w.Fields {
			f, err := wireToField(&w.Fields[i])
			if err != nil {
				return nil, err
			}
			fields = append(fields, f)
		}
		return arrow.StructOf(fields...), nil
	default:
		return nil, fmt.Errorf("unsupported data type kind %q", w.Kind)
	}
}

func parseTimestampUnit(unit string) (arrow.TimeUnit, error) {
	switch unit {
	case arrow.Second.String():
		return arrow.Second, nil
	case arrow.Millisecond.String():
		return arrow.Millisecond, nil
	case arrow.Microsecond.String():
		return arrow.Microsecond, nil
	case arrow.Nanosecond.String():
		return arrow.Nanosecond, nil
	default:
		return 0, fmt.Errorf("unsupported timestamp unit %q", unit)
	}
}

func metadataToEntries(m arrow.Metadata) []metadataEntry {
	if len(m.Keys()) == 0 {
		return nil
	}

	keys := append([]string(nil), m.Keys()...)
	sort.Strings(keys)

	entries := make([]metadataEntry, 0, len(keys))
	for _, key := range keys {
		value, _ := m.GetValue(key)
		entries = append(entries, metadataEntry{Key: key, Value: value})
	}

	return entries
}

func entriesToMetadata(entries []metadataEntry) arrow.Metadata {
	keys := make([]string, 0, len(entries))
	values := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.Key)
		values = append(values, entry.Value)
	}

	return arrow.NewMetadata(keys, values)
}

// pathTemplateData holds the data available for path template execution.
type pathTemplateData struct {
	Table       string
	Schema      string
	SegmentID   string
	Extension   string
	Year        int
	Month       int
	MonthPadded string
	Day         int
	DayPadded   string
}

// segmentRemoteKey returns the remote object key for a segment.
func segmentRemoteKey(
	tmpl *template.Template,
	tableEsc, fingerprint, segmentID, ext string,
	sealTime time.Time,
) (string, error) {
	utc := sealTime.UTC()
	year, month, day := utc.Date()

	data := pathTemplateData{
		Table:       tableEsc,
		Schema:      fingerprint,
		SegmentID:   segmentID,
		Extension:   ext,
		Year:        year,
		Month:       int(month),
		MonthPadded: fmt.Sprintf("%02d", month),
		Day:         day,
		DayPadded:   fmt.Sprintf("%02d", day),
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("executing path template: %w", err)
	}

	return buf.String(), nil
}
