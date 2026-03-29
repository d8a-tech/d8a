package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeWriteBatch struct {
	appendErrOnCall int
	appendErr       error
	sendErr         error
	appendCalls     int
	sendCalls       int
}

func (b *fakeWriteBatch) Append(_ ...any) error {
	b.appendCalls++
	if b.appendErrOnCall > 0 && b.appendCalls == b.appendErrOnCall {
		return b.appendErr
	}

	return nil
}

func (b *fakeWriteBatch) Send() error {
	b.sendCalls++
	return b.sendErr
}

type capturingWriteBatch struct {
	rows [][]any
}

func (b *capturingWriteBatch) Append(v ...any) error {
	b.rows = append(b.rows, v)
	return nil
}

func (b *capturingWriteBatch) Send() error {
	return nil
}

func TestClickHouseDriverWriteBatchErrorSemantics(t *testing.T) {
	sendErr := errors.New("send failed")
	appendErr := errors.New("append failed")

	testCases := []struct {
		name          string
		batch         *fakeWriteBatch
		expectedErr   error
		errContains   string
		expectedSends int
	}{
		{
			name: "returns send error",
			batch: &fakeWriteBatch{
				sendErr: sendErr,
			},
			expectedErr:   sendErr,
			errContains:   "error sending batch",
			expectedSends: 1,
		},
		{
			name: "does not send after append failure",
			batch: &fakeWriteBatch{
				appendErrOnCall: 2,
				appendErr:       appendErr,
			},
			expectedErr:   appendErr,
			errContains:   "error appending row to batch",
			expectedSends: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// given
			tableName := "events"
			schemaFields := []arrow.Field{{Name: "count", Type: arrow.PrimitiveTypes.Int64}}
			schema := arrow.NewSchema(schemaFields, nil)
			rows := []map[string]any{{"count": int64(1)}, {"count": int64(2)}}

			driver := &clickhouseDriver{
				database:        "testdb",
				fieldTypeMapper: NewFieldTypeMapper(),
				tableColumnsCache: util.NewTTLCache[[]*arrow.Field](
					time.Minute,
				),
				prepareBatch: func(_ context.Context, _ string) (clickhouseWriteBatch, error) {
					return tc.batch, nil
				},
			}

			realFields := []*arrow.Field{&schemaFields[0]}
			driver.tableColumnsCache.Set(tableName, realFields)

			// when
			err := driver.Write(context.Background(), tableName, schema, rows)

			// then
			assert.Error(t, err)
			assert.ErrorContains(t, err, tc.errContains)
			assert.ErrorIs(t, err, tc.expectedErr)
			assert.Equal(t, tc.expectedSends, tc.batch.sendCalls)
		})
	}
}

func TestWrite_MetadataAlignedWithReorderedFields(t *testing.T) {
	// Regression test for H10: when ClickHouse physical column order differs
	// from the Arrow schema order, metadata must come from the reordered field,
	// not the original Arrow schema index.
	//
	// Arrow schema order:  [timestamp (DateTime64 with precision metadata), name (String)]
	// Physical CH order:   [name (String), timestamp (DateTime64 with precision metadata)]
	//
	// Before the fix, Format() for physical column 0 ("name") would receive
	// schema.Field(0).Metadata — the timestamp's precision metadata instead of
	// the string's empty metadata, and vice versa for column 1.

	// given
	tableName := "events"

	// Arrow schema: timestamp first, name second
	timestampMeta := arrow.NewMetadata(
		[]string{PrecisionMetadataKey},
		[]string{PrecisionMetadataValueSecond},
	)
	arrowFields := []arrow.Field{
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_s, Metadata: timestampMeta},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(arrowFields, nil)

	// Physical ClickHouse order: name first, timestamp second (reversed)
	nameField := &arrow.Field{Name: "name", Type: arrow.BinaryTypes.String}
	tsField := &arrow.Field{
		Name:     "timestamp",
		Type:     arrow.FixedWidthTypes.Timestamp_s,
		Metadata: timestampMeta,
	}
	physicalFields := []*arrow.Field{nameField, tsField}

	batch := &capturingWriteBatch{}
	driver := &clickhouseDriver{
		database:        "testdb",
		fieldTypeMapper: NewFieldTypeMapper(),
		tableColumnsCache: util.NewTTLCache[[]*arrow.Field](
			time.Minute,
		),
		prepareBatch: func(_ context.Context, _ string) (clickhouseWriteBatch, error) {
			return batch, nil
		},
	}
	driver.tableColumnsCache.Set(tableName, physicalFields)

	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	rows := []map[string]any{
		{"name": "pageview", "timestamp": ts},
	}

	// when
	err := driver.Write(context.Background(), tableName, schema, rows)

	// then
	require.NoError(t, err)
	require.Len(t, batch.rows, 1)

	// Physical order is [name, timestamp] — values must match that order
	appended := batch.rows[0]
	require.Len(t, appended, 2)
	assert.Equal(t, "pageview", appended[0], "first physical column should be name")
	assert.Equal(t, ts.Format(timestampFormat), appended[1], "second physical column should be formatted timestamp")
}
