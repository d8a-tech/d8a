package files

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockKV struct {
	mu   sync.Mutex
	data map[string][]byte
}

type mockClosableKV struct {
	*mockKV
	closeErr error
	closed   bool
}

func newMockKV() *mockKV {
	return &mockKV{data: map[string][]byte{}}
}

func newMockClosableKV(closeErr error) *mockClosableKV {
	return &mockClosableKV{mockKV: newMockKV(), closeErr: closeErr}
}

func (m *mockClosableKV) Close() error {
	m.closed = true
	return m.closeErr
}

func (m *mockKV) Get(key []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

func (m *mockKV) Set(key, value []byte, opts ...storage.SetOptionsFunc) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	options := storage.DefaultSetOptions()
	for _, opt := range opts {
		opt(options)
	}

	strKey := string(key)
	existing, exists := m.data[strKey]
	if exists && options.SkipIfKeyAlreadyExists {
		if options.ReturnPreviousValue {
			out := make([]byte, len(existing))
			copy(out, existing)
			return out, nil
		}
		return nil, nil
	}

	stored := make([]byte, len(value))
	copy(stored, value)
	m.data[strKey] = stored

	if options.ReturnPreviousValue && exists {
		out := make([]byte, len(existing))
		copy(out, existing)
		return out, nil
	}

	return nil, nil
}

func (m *mockKV) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

type mockUpload struct {
	buf       *bytes.Buffer
	key       string
	commits   int
	aborts    int
	errCommit error
}

func (m *mockUpload) Writer() io.Writer { return m.buf }

func (m *mockUpload) Commit() error {
	m.commits++
	return m.errCommit
}

func (m *mockUpload) Abort() error {
	m.aborts++
	return nil
}

type uploadRecord struct {
	key    string
	bytes  []byte
	upload *mockUpload
}

type mockStreamUploader struct {
	mu            sync.Mutex
	uploads       []*uploadRecord
	nextCommitErr error
}

func (m *mockStreamUploader) Begin(ctx context.Context, key string) (Upload, error) {
	_ = ctx
	b := &bytes.Buffer{}
	u := &mockUpload{buf: b, key: key, errCommit: m.nextCommitErr}
	rec := &uploadRecord{key: key, upload: u}

	m.mu.Lock()
	m.uploads = append(m.uploads, rec)
	m.mu.Unlock()

	return &recordingUpload{upload: u, rec: rec}, nil
}

type recordingUpload struct {
	upload *mockUpload
	rec    *uploadRecord
}

func (r *recordingUpload) Writer() io.Writer { return r.upload.Writer() }
func (r *recordingUpload) Abort() error      { return r.upload.Abort() }
func (r *recordingUpload) Commit() error {
	err := r.upload.Commit()
	r.rec.bytes = append([]byte(nil), r.upload.buf.Bytes()...)
	return err
}

type failFormat struct{ err error }

func (f *failFormat) Extension() string { return "csv" }

func (f *failFormat) NewWriter(w io.Writer, schema *arrow.Schema) (FormatWriter, error) {
	_ = w
	_ = schema
	return &failFormatWriter{err: f.err}, nil
}

type failFormatWriter struct{ err error }

func (w *failFormatWriter) WriteRows(rows []map[string]any) error {
	_ = rows
	return w.err
}
func (w *failFormatWriter) Close() error { return nil }

type closeErrSpool struct {
	closeErr error
}

func (s *closeErrSpool) Append(key string, payload []byte) error {
	_ = key
	_ = payload
	return nil
}

func (s *closeErrSpool) Flush(fn func(key string, next func() ([][]byte, error)) error) error {
	_ = fn
	return nil
}

func (s *closeErrSpool) Recover() error {
	return nil
}

func (s *closeErrSpool) Close() error {
	return s.closeErr
}

func mustSpool(t *testing.T) spools.Spool {
	t.Helper()
	s, err := spools.New(afero.NewMemMapFs(), "/spool")
	require.NoError(t, err)
	return s
}

func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
}

func TestSpoolDriver_WriteStoresSchemaAndAppendsPayload(t *testing.T) {
	ctx := context.Background()
	spool := mustSpool(t)
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	driver := NewSpoolDriver(ctx, spool, kv, uploader, NewCSVFormat(), withManualCycle())

	schema := testSchema()
	rows := []map[string]any{{"id": int64(1)}}

	err := driver.Write(ctx, "events", schema, rows)
	require.NoError(t, err)

	fingerprint := schemaFingerprint(schema)
	schemaBytes, err := kv.Get([]byte(fingerprint))
	require.NoError(t, err)
	require.NotEmpty(t, schemaBytes)

	var gotFrames [][]byte
	err = spool.Flush(func(key string, next func() ([][]byte, error)) error {
		table, fp, parseErr := parseSpoolKey(key)
		require.NoError(t, parseErr)
		assert.Equal(t, "events", table)
		assert.Equal(t, fingerprint, fp)
		for {
			frames, nextErr := next()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			require.NoError(t, nextErr)
			gotFrames = append(gotFrames, frames...)
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, gotFrames, 1)

	var decoded []map[string]any
	require.NoError(t, json.Unmarshal(gotFrames[0], &decoded))
	assert.Len(t, decoded, 1)
	assert.Equal(t, float64(1), decoded[0]["id"])
}

func TestSpoolDriver_FlushOnceStreamsMultipleWritesAndCommits(t *testing.T) {
	ctx := context.Background()
	spool := mustSpool(t)
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	driver := NewSpoolDriver(ctx, spool, kv, uploader, NewCSVFormat(), withManualCycle())

	schema := testSchema()
	require.NoError(t, driver.Write(ctx, "events", schema, []map[string]any{{"id": int64(1)}}))
	require.NoError(t, driver.Write(ctx, "events", schema, []map[string]any{{"id": int64(2)}}))

	require.NoError(t, driver.flushOnce(ctx))

	uploader.mu.Lock()
	defer uploader.mu.Unlock()
	require.Len(t, uploader.uploads, 1)
	rec := uploader.uploads[0]
	assert.Equal(t, 1, rec.upload.commits)
	assert.Equal(t, 0, rec.upload.aborts)

	content := string(rec.bytes)
	assert.Contains(t, content, "id")
	assert.Contains(t, content, "1")
	assert.Contains(t, content, "2")
}

func TestSpoolDriver_FlushOnceAbortsOnFormatWriterError(t *testing.T) {
	ctx := context.Background()
	spool := mustSpool(t)
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	driver := NewSpoolDriver(ctx, spool, kv, uploader, &failFormat{err: errors.New("write failed")}, withManualCycle())

	schema := testSchema()
	require.NoError(t, driver.Write(ctx, "events", schema, []map[string]any{{"id": int64(1)}}))

	err := driver.flushOnce(ctx)
	require.Error(t, err)

	uploader.mu.Lock()
	defer uploader.mu.Unlock()
	require.Len(t, uploader.uploads, 1)
	assert.Equal(t, 0, uploader.uploads[0].upload.commits)
	assert.Equal(t, 1, uploader.uploads[0].upload.aborts)
}

func TestSpoolDriver_FlushOnceAbortsOnCommitError(t *testing.T) {
	ctx := context.Background()
	spool := mustSpool(t)
	kv := newMockKV()
	uploader := &mockStreamUploader{nextCommitErr: errors.New("commit failed")}
	driver := NewSpoolDriver(ctx, spool, kv, uploader, NewCSVFormat(), withManualCycle())

	schema := testSchema()
	require.NoError(t, driver.Write(ctx, "events", schema, []map[string]any{{"id": int64(1)}}))

	err := driver.flushOnce(ctx)
	require.Error(t, err)

	uploader.mu.Lock()
	defer uploader.mu.Unlock()
	require.Len(t, uploader.uploads, 1)
	assert.Equal(t, 1, uploader.uploads[0].upload.commits)
	assert.Equal(t, 1, uploader.uploads[0].upload.aborts)
}

func TestSpoolDriver_RepeatedWriteSameSchemaIsIdempotentKVSet(t *testing.T) {
	ctx := context.Background()
	spool := mustSpool(t)
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	driver := NewSpoolDriver(ctx, spool, kv, uploader, NewCSVFormat(), withManualCycle())

	schema := testSchema()
	require.NoError(t, driver.Write(ctx, "events", schema, []map[string]any{{"id": int64(1)}}))
	require.NoError(t, driver.Write(ctx, "events", schema, []map[string]any{{"id": int64(2)}}))
}

func TestSpoolDriver_CloseFlushOnClose(t *testing.T) {
	ctx := context.Background()
	spool := mustSpool(t)
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	driver := NewSpoolDriver(ctx, spool, kv, uploader, NewCSVFormat(), withManualCycle(), WithFlushOnClose(true))

	schema := testSchema()
	require.NoError(t, driver.Write(ctx, "events", schema, []map[string]any{{"id": int64(1)}}))

	require.NoError(t, driver.Close())

	uploader.mu.Lock()
	defer uploader.mu.Unlock()
	require.Len(t, uploader.uploads, 1)
	assert.Equal(t, 1, uploader.uploads[0].upload.commits)
}

func TestSpoolDriver_PathTemplateAffectsRemoteKey(t *testing.T) {
	ctx := context.Background()
	spool := mustSpool(t)
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	driver := NewSpoolDriver(
		ctx,
		spool,
		kv,
		uploader,
		NewCSVFormat(),
		withManualCycle(),
		WithPathTemplate("custom/{{.Table}}/{{.Schema}}/{{.SegmentID}}.{{.Extension}}"),
	)

	schema := testSchema()
	require.NoError(t, driver.Write(ctx, "events", schema, []map[string]any{{"id": int64(1)}}))
	require.NoError(t, driver.flushOnce(ctx))

	uploader.mu.Lock()
	defer uploader.mu.Unlock()
	require.Len(t, uploader.uploads, 1)

	key := uploader.uploads[0].key
	assert.Contains(t, key, "custom/events/")
	assert.Contains(t, key, ".csv")
	assert.Contains(t, key, schemaFingerprint(schema))
}

func TestSpoolDriver_StartTimerFlushesPeriodically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spool := mustSpool(t)
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	driver := NewSpoolDriver(ctx, spool, kv, uploader, NewCSVFormat(), WithFlushInterval(10*time.Millisecond))

	schema := testSchema()
	require.NoError(t, driver.Write(ctx, "events", schema, []map[string]any{{"id": int64(1)}}))

	require.Eventually(t, func() bool {
		uploader.mu.Lock()
		defer uploader.mu.Unlock()
		return len(uploader.uploads) > 0
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, driver.Close())
}

func TestSpoolDriver_CloseClosesKVWhenClosable(t *testing.T) {
	ctx := context.Background()
	spool := mustSpool(t)
	kv := newMockClosableKV(nil)
	uploader := &mockStreamUploader{}
	driver := NewSpoolDriver(ctx, spool, kv, uploader, NewCSVFormat(), withManualCycle())

	require.NoError(t, driver.Close())
	assert.True(t, kv.closed)
}

func TestSpoolDriver_CloseReturnsJoinedErrorWhenSpoolAndKVCloseFail(t *testing.T) {
	ctx := context.Background()
	spoolErr := errors.New("spool close failed")
	kvErr := errors.New("kv close failed")

	driver := NewSpoolDriver(
		ctx,
		&closeErrSpool{closeErr: spoolErr},
		newMockClosableKV(kvErr),
		&mockStreamUploader{},
		NewCSVFormat(),
		withManualCycle(),
	)

	err := driver.Close()
	require.Error(t, err)
	assert.ErrorIs(t, err, spoolErr)
	assert.ErrorIs(t, err, kvErr)
}
