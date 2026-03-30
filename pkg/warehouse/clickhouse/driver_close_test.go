package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConn is a minimal implementation of clickhouse.Conn (chdriver.Conn)
// that tracks whether Close was called and can return a configurable error.
type fakeConn struct {
	closed   bool
	closeErr error
}

func (c *fakeConn) Close() error {
	c.closed = true
	return c.closeErr
}

func (c *fakeConn) Contributors() []string { return nil }
func (c *fakeConn) ServerVersion() (*chdriver.ServerVersion, error) {
	return nil, nil //nolint:nilnil // test stub
}
func (c *fakeConn) Select(context.Context, any, string, ...any) error { return nil }
func (c *fakeConn) Query(context.Context, string, ...any) (chdriver.Rows, error) {
	return nil, nil //nolint:nilnil // test stub
}
func (c *fakeConn) QueryRow(context.Context, string, ...any) chdriver.Row   { return nil }
func (c *fakeConn) Exec(context.Context, string, ...any) error              { return nil }
func (c *fakeConn) AsyncInsert(context.Context, string, bool, ...any) error { return nil }
func (c *fakeConn) Ping(context.Context) error                              { return nil }
func (c *fakeConn) Stats() chdriver.Stats                                   { return chdriver.Stats{} }
func (c *fakeConn) PrepareBatch(context.Context, string, ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	return nil, nil //nolint:nilnil // test stub
}

// noopSQLDriver / noopSQLConn provide a trivial database/sql driver
// so we can create a real *sql.DB without external dependencies.
type noopSQLDriver struct{}

func (d *noopSQLDriver) Open(_ string) (driver.Conn, error) {
	return &noopSQLConn{}, nil
}

type noopSQLConn struct{}

func (c *noopSQLConn) Prepare(_ string) (driver.Stmt, error) { return nil, nil } //nolint:nilnil // test stub
func (c *noopSQLConn) Close() error                          { return nil }
func (c *noopSQLConn) Begin() (driver.Tx, error)             { return nil, nil } //nolint:nilnil // test stub

var registerOnce sync.Once

func newTestSQLDB(t *testing.T) *sql.DB {
	t.Helper()
	registerOnce.Do(func() {
		sql.Register("noop_clickhouse_close_test", &noopSQLDriver{})
	})
	db, err := sql.Open("noop_clickhouse_close_test", "")
	require.NoError(t, err)
	return db
}

func TestClose_ClosesBothConnections(t *testing.T) {
	// given
	conn := &fakeConn{}
	db := newTestSQLDB(t)

	d := &clickhouseDriver{
		db:   db,
		conn: conn,
	}

	// when
	err := d.Close()

	// then
	require.NoError(t, err)
	assert.True(t, conn.closed, "clickhouse.Conn should be closed")
}

func TestClose_ReturnsConnError(t *testing.T) {
	// given
	connErr := errors.New("conn close failed")
	conn := &fakeConn{closeErr: connErr}
	db := newTestSQLDB(t)

	d := &clickhouseDriver{
		db:   db,
		conn: conn,
	}

	// when
	err := d.Close()

	// then
	require.Error(t, err)
	assert.ErrorIs(t, err, connErr)
	assert.True(t, conn.closed)
}
