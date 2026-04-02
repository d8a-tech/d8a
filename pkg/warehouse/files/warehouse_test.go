package files

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/warehouse/testutils"
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

type stubSpool struct {
	appendKey     string
	appendPayload []byte
	appendErr     error
}

func (s *stubSpool) Append(key string, payload []byte) error {
	s.appendKey = key
	s.appendPayload = append([]byte(nil), payload...)
	return s.appendErr
}

type immediateFlushSpool struct {
	handler       spools.FlushHandler
	appendKey     string
	appendPayload []byte
	appendErr     error
}

func (s *immediateFlushSpool) setHandler(handler spools.FlushHandler) {
	s.handler = handler
}

func (s *immediateFlushSpool) Append(key string, payload []byte) error {
	s.appendKey = key
	s.appendPayload = append([]byte(nil), payload...)
	if s.appendErr != nil {
		return s.appendErr
	}

	if s.handler == nil {
		return errors.New("flush handler is not set")
	}

	called := false
	err := s.handler(key, func() ([][]byte, error) {
		if called {
			return nil, io.EOF
		}
		called = true
		return [][]byte{append([]byte(nil), payload...)}, nil
	})
	if err != nil {
		return err
	}

	return nil
}

type stubFactory struct {
	createErr error
	spool     spools.Spool
	handler   spools.FlushHandler
}

func (f *stubFactory) Create(handler spools.FlushHandler) (spools.Spool, error) {
	f.handler = handler
	if f.createErr != nil {
		return nil, f.createErr
	}
	if spoolWithHandler, ok := f.spool.(interface{ setHandler(spools.FlushHandler) }); ok {
		spoolWithHandler.setHandler(handler)
	}
	return f.spool, nil
}

func (f *stubFactory) Close() error {
	return nil
}

func testSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
}

func nextFromFrames(frames ...[]byte) func() ([][]byte, error) {
	index := 0
	return func() ([][]byte, error) {
		if index >= len(frames) {
			return nil, io.EOF
		}
		frame := frames[index]
		index++
		return [][]byte{frame}, nil
	}
}

func TestNewFilesDriver_ReturnsErrorWhenFactoryCreateFails(t *testing.T) {
	factoryErr := errors.New("create failed")

	driver, err := NewFilesDriver(
		context.Background(),
		&stubFactory{createErr: factoryErr},
		newMockKV(),
		&mockStreamUploader{},
		NewCSVFormat(),
	)

	require.Error(t, err)
	assert.Nil(t, driver)
	assert.ErrorIs(t, err, factoryErr)
}

func TestFilesDriver_SharedBasicWrites(t *testing.T) {
	tests := []struct {
		name      string
		format    Format
		extSuffix string
		useShared bool
	}{
		{
			name:      "csv",
			format:    NewCSVFormat(),
			extSuffix: ".csv",
			useShared: true,
		},
		{
			name:      "csv_gzip",
			format:    NewCSVFormat(WithCompression(Gzip(gzip.BestSpeed))),
			extSuffix: ".csv.gz",
			useShared: true,
		},
		{
			name:      "parquet",
			format:    NewParquetFormat(),
			extSuffix: ".parquet",
			useShared: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spool := &immediateFlushSpool{}
			uploader := &mockStreamUploader{}
			driver, err := NewFilesDriver(
				context.Background(),
				&stubFactory{spool: spool},
				newMockKV(),
				uploader,
				tt.format,
			)
			require.NoError(t, err)

			testutils.TestBasicWrites(t, driver, "events")

			uploader.mu.Lock()
			defer uploader.mu.Unlock()
			require.Len(t, uploader.uploads, 1)
			assert.Equal(t, 1, uploader.uploads[0].upload.commits)
			assert.Equal(t, 0, uploader.uploads[0].upload.aborts)
			assert.Contains(t, uploader.uploads[0].key, tt.extSuffix)
		})
	}
}

func TestFilesDriver_WriteStoresSchemaAndAppendsPayload(t *testing.T) {
	ctx := context.Background()
	spool := &stubSpool{}
	factory := &stubFactory{spool: spool}
	kv := newMockKV()
	driver, err := NewFilesDriver(ctx, factory, kv, &mockStreamUploader{}, NewCSVFormat())
	require.NoError(t, err)

	schema := testSchema()
	rows := []map[string]any{{"id": int64(1)}}

	err = driver.Write(ctx, "events", schema, rows)
	require.NoError(t, err)

	fingerprint := schemaFingerprint(schema)
	schemaBytes, err := kv.Get([]byte(fingerprint))
	require.NoError(t, err)
	require.NotEmpty(t, schemaBytes)

	assert.Equal(t, "events/"+fingerprint, spool.appendKey)

	var decoded []map[string]any
	require.NoError(t, json.Unmarshal(spool.appendPayload, &decoded))
	assert.Len(t, decoded, 1)
	assert.Equal(t, float64(1), decoded[0]["id"])
}

func TestFilesDriver_FlushHandlerStreamsMultipleFramesAndCommits(t *testing.T) {
	ctx := context.Background()
	spool := &stubSpool{}
	factory := &stubFactory{spool: spool}
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	driver, err := NewFilesDriver(ctx, factory, kv, uploader, NewCSVFormat())
	require.NoError(t, err)

	schema := testSchema()
	fingerprint := schemaFingerprint(schema)
	schemaBytes, err := marshalSchema(schema)
	require.NoError(t, err)
	_, err = kv.Set([]byte(fingerprint), schemaBytes)
	require.NoError(t, err)

	frame1, err := json.Marshal([]map[string]any{{"id": int64(1)}})
	require.NoError(t, err)
	frame2, err := json.Marshal([]map[string]any{{"id": int64(2)}})
	require.NoError(t, err)

	err = factory.handler("events/"+fingerprint, nextFromFrames(frame1, frame2))
	require.NoError(t, err)

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
	_ = driver
}

func TestFilesDriver_FlushHandlerAbortsOnFormatWriterError(t *testing.T) {
	ctx := context.Background()
	factory := &stubFactory{spool: &stubSpool{}}
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	_, err := NewFilesDriver(ctx, factory, kv, uploader, &failFormat{err: errors.New("write failed")})
	require.NoError(t, err)

	schema := testSchema()
	fingerprint := schemaFingerprint(schema)
	schemaBytes, err := marshalSchema(schema)
	require.NoError(t, err)
	_, err = kv.Set([]byte(fingerprint), schemaBytes)
	require.NoError(t, err)

	frame, err := json.Marshal([]map[string]any{{"id": int64(1)}})
	require.NoError(t, err)

	err = factory.handler("events/"+fingerprint, nextFromFrames(frame))
	require.Error(t, err)

	uploader.mu.Lock()
	defer uploader.mu.Unlock()
	require.Len(t, uploader.uploads, 1)
	assert.Equal(t, 0, uploader.uploads[0].upload.commits)
	assert.Equal(t, 1, uploader.uploads[0].upload.aborts)
}

func TestFilesDriver_FlushHandlerAbortsOnCommitError(t *testing.T) {
	ctx := context.Background()
	factory := &stubFactory{spool: &stubSpool{}}
	kv := newMockKV()
	uploader := &mockStreamUploader{nextCommitErr: errors.New("commit failed")}
	_, err := NewFilesDriver(ctx, factory, kv, uploader, NewCSVFormat())
	require.NoError(t, err)

	schema := testSchema()
	fingerprint := schemaFingerprint(schema)
	schemaBytes, err := marshalSchema(schema)
	require.NoError(t, err)
	_, err = kv.Set([]byte(fingerprint), schemaBytes)
	require.NoError(t, err)

	frame, err := json.Marshal([]map[string]any{{"id": int64(1)}})
	require.NoError(t, err)

	err = factory.handler("events/"+fingerprint, nextFromFrames(frame))
	require.Error(t, err)

	uploader.mu.Lock()
	defer uploader.mu.Unlock()
	require.Len(t, uploader.uploads, 1)
	assert.Equal(t, 1, uploader.uploads[0].upload.commits)
	assert.Equal(t, 1, uploader.uploads[0].upload.aborts)
}

func TestFilesDriver_PathTemplateAffectsRemoteKey(t *testing.T) {
	ctx := context.Background()
	factory := &stubFactory{spool: &stubSpool{}}
	kv := newMockKV()
	uploader := &mockStreamUploader{}
	driver, err := NewFilesDriver(
		ctx,
		factory,
		kv,
		uploader,
		NewCSVFormat(),
		WithPathTemplate("custom/{{.Table}}/{{.Schema}}/{{.SegmentID}}.{{.Extension}}"),
	)
	require.NoError(t, err)

	schema := testSchema()
	fingerprint := schemaFingerprint(schema)
	schemaBytes, err := marshalSchema(schema)
	require.NoError(t, err)
	_, err = kv.Set([]byte(fingerprint), schemaBytes)
	require.NoError(t, err)

	frame, err := json.Marshal([]map[string]any{{"id": int64(1)}})
	require.NoError(t, err)

	err = factory.handler("events/"+fingerprint, nextFromFrames(frame))
	require.NoError(t, err)

	uploader.mu.Lock()
	defer uploader.mu.Unlock()
	require.Len(t, uploader.uploads, 1)

	key := uploader.uploads[0].key
	assert.Contains(t, key, "custom/events/")
	assert.Contains(t, key, ".csv")
	assert.Contains(t, key, schemaFingerprint(schema))
	_ = driver
}

func TestFilesDriver_CloseClosesKVWhenClosable(t *testing.T) {
	ctx := context.Background()
	kv := newMockClosableKV(nil)
	driver, err := NewFilesDriver(ctx, &stubFactory{spool: &stubSpool{}}, kv, &mockStreamUploader{}, NewCSVFormat())
	require.NoError(t, err)

	require.NoError(t, driver.Close())
	assert.True(t, kv.closed)
}

func TestFilesDriver_CloseReturnsKVCloseError(t *testing.T) {
	ctx := context.Background()
	kvErr := errors.New("kv close failed")
	driver, err := NewFilesDriver(
		ctx,
		&stubFactory{spool: &stubSpool{}},
		newMockClosableKV(kvErr),
		&mockStreamUploader{},
		NewCSVFormat(),
	)
	require.NoError(t, err)

	err = driver.Close()
	require.Error(t, err)
	assert.ErrorIs(t, err, kvErr)
}
