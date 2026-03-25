package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/util"
	"github.com/stretchr/testify/assert"
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
