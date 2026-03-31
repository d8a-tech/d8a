package files

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"text/template"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

// SpoolOption is a functional option for configuring SpoolDriver.
type SpoolOption func(*SpoolDriver)

// withManualCycle disables automatic timer-based flush cycles (test-only).
func withManualCycle() SpoolOption {
	return func(sd *SpoolDriver) {
		sd.sealCheckInterval = 0
	}
}

// WithMaxSegmentSize sets the maximum active spool file size in bytes before
// rotation. Maps to spools.WithMaxActiveSize.
func WithMaxSegmentSize(n int64) SpoolOption {
	return func(sd *SpoolDriver) {
		sd.maxSegmentSize = n
	}
}

// WithMaxSegmentAge is accepted for configuration compatibility but has no
// effect in the new spool-based design. Active file rotation is driven solely
// by size; periodic flushing is handled by the seal-check timer.
func WithMaxSegmentAge(_ time.Duration) SpoolOption {
	return func(*SpoolDriver) {}
}

// WithSealCheckInterval sets how often to run periodic flushes.
func WithSealCheckInterval(d time.Duration) SpoolOption {
	return func(sd *SpoolDriver) {
		sd.sealCheckInterval = d
	}
}

// WithFlushOnClose configures whether Close forces a final flush.
func WithFlushOnClose(v bool) SpoolOption {
	return func(sd *SpoolDriver) {
		sd.flushOnClose = v
	}
}

// WithPathTemplate sets the path template string for remote object keys.
func WithPathTemplate(tmplStr string) SpoolOption {
	return func(sd *SpoolDriver) {
		sd.pathTemplateStr = tmplStr
	}
}

// ticker abstracts time.Ticker to allow deterministic testing.
type ticker interface {
	C() <-chan time.Time
	Stop()
}

// realTicker wraps *time.Ticker to implement ticker.
type realTicker struct {
	t *time.Ticker
}

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

// SpoolDriver is a warehouse.Driver that serializes analytics rows into a
// pkg/spools spool and periodically flushes them to object storage via Format
// encoding and an Uploader.
type SpoolDriver struct {
	ctx               context.Context
	uploader          Uploader
	format            Format
	ext               string // file extension without leading dot
	spoolDir          string
	pathTemplate      *template.Template
	pathTemplateStr   string
	maxSegmentSize    int64
	sealCheckInterval time.Duration
	stopCh            chan struct{}
	stopOnce          sync.Once
	wg                sync.WaitGroup
	spool             spools.Spool
	newTicker         func(time.Duration) ticker
	flushOnClose      bool

	// schemas caches the most recent arrow.Schema per spool key so the
	// flush callback can encode the final output format. Entries are set
	// on Write and read during flush.
	schemas   map[string]*arrow.Schema
	schemasMu sync.RWMutex
}

var _ warehouse.Driver = (*SpoolDriver)(nil)

// spoolFrame is the self-describing payload serialized into each spool frame.
// It carries enough metadata for the flush callback to produce a formatted
// upload file without consulting any in-memory maps.
type spoolFrame struct {
	TableEsc    string
	Fingerprint string
	Schema      []serializedField
	Rows        []map[string]any
}

// serializedField is a gob-friendly representation of an arrow.Field.
type serializedField struct {
	Name    string
	TypeStr string
}

func init() {
	// Register map[string]any and common value types for gob encoding
	// so that spoolFrame.Rows can be round-tripped.
	gob.Register(map[string]any{})
	gob.Register(time.Time{})
	gob.Register([]any{})
	gob.Register(int64(0))
	gob.Register(float64(0))
}

func encodeFrame(f *spoolFrame) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(f); err != nil {
		return nil, fmt.Errorf("encoding spool frame: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeFrame(data []byte) (*spoolFrame, error) {
	var f spoolFrame
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&f); err != nil {
		return nil, fmt.Errorf("decoding spool frame: %w", err)
	}
	return &f, nil
}

// rebuildSchema reconstructs an arrow.Schema from serialized fields.
// Only types actually used in warehouse output are supported.
func rebuildSchema(fields []serializedField) *arrow.Schema {
	arrowFields := make([]arrow.Field, len(fields))
	for i, sf := range fields {
		arrowFields[i] = arrow.Field{
			Name: sf.Name,
			Type: arrowTypeFromString(sf.TypeStr),
		}
	}
	return arrow.NewSchema(arrowFields, nil)
}

func serializeFields(schema *arrow.Schema) []serializedField {
	fields := make([]serializedField, len(schema.Fields()))
	for i, f := range schema.Fields() {
		fields[i] = serializedField{
			Name:    f.Name,
			TypeStr: f.Type.String(),
		}
	}
	return fields
}

// arrowTypeFromString returns the arrow.DataType for a type string.
// Falls back to string for unknown types — the Format layer handles
// conversion to the wire representation.
func arrowTypeFromString(s string) arrow.DataType {
	switch s {
	case "int8":
		return arrow.PrimitiveTypes.Int8
	case "int16":
		return arrow.PrimitiveTypes.Int16
	case "int32":
		return arrow.PrimitiveTypes.Int32
	case "int64":
		return arrow.PrimitiveTypes.Int64
	case "uint8":
		return arrow.PrimitiveTypes.Uint8
	case "uint16":
		return arrow.PrimitiveTypes.Uint16
	case "uint32":
		return arrow.PrimitiveTypes.Uint32
	case "uint64":
		return arrow.PrimitiveTypes.Uint64
	case "float32":
		return arrow.PrimitiveTypes.Float32
	case "float64":
		return arrow.PrimitiveTypes.Float64
	case "bool":
		return arrow.FixedWidthTypes.Boolean
	case "utf8":
		return arrow.BinaryTypes.String
	case "timestamp[ns, tz=UTC]":
		return arrow.FixedWidthTypes.Timestamp_ns
	case "timestamp[us, tz=UTC]":
		return arrow.FixedWidthTypes.Timestamp_us
	case "timestamp[ms, tz=UTC]":
		return arrow.FixedWidthTypes.Timestamp_ms
	case "timestamp[s, tz=UTC]":
		return arrow.FixedWidthTypes.Timestamp_s
	case "date32":
		return arrow.FixedWidthTypes.Date32
	case "date64":
		return arrow.FixedWidthTypes.Date64
	default:
		return arrow.BinaryTypes.String
	}
}

// NewSpoolDriver creates a new spool driver that serializes rows into a
// pkg/spools spool and periodically flushes formatted files to object storage.
func NewSpoolDriver(
	ctx context.Context,
	uploader Uploader,
	format Format,
	spoolDir string,
	opts ...SpoolOption,
) *SpoolDriver {
	sd := &SpoolDriver{
		ctx:               ctx,
		uploader:          uploader,
		format:            format,
		ext:               format.Extension(),
		spoolDir:          spoolDir,
		maxSegmentSize:    1 << 30, // 1 GiB
		sealCheckInterval: 15 * time.Second,
		stopCh:            make(chan struct{}),
		schemas:           make(map[string]*arrow.Schema),
	}

	sd.newTicker = func(d time.Duration) ticker {
		return &realTicker{time.NewTicker(d)}
	}

	for _, opt := range opts {
		opt(sd)
	}

	// Parse path template: use default if not set via option
	tmplStr := sd.pathTemplateStr
	if tmplStr == "" {
		tmplStr = "table={{.Table}}/schema={{.Schema}}/y={{.Year}}/m={{.MonthPadded}}/d={{.DayPadded}}/" +
			"{{.SegmentID}}.{{.Extension}}"
	}
	tmpl, err := parsePathTemplate(tmplStr)
	if err != nil {
		logrus.WithError(err).Panic("failed to parse path template")
	}
	sd.pathTemplate = tmpl

	// Construct the underlying spool with quarantine strategy and failure
	// threshold of 3 to match prior behavior.
	var spoolOpts []spools.Option
	spoolOpts = append(spoolOpts,
		spools.WithFailureStrategy(spools.NewQuarantineStrategy()),
		spools.WithMaxFailures(3),
	)
	if sd.maxSegmentSize > 0 {
		spoolOpts = append(spoolOpts, spools.WithMaxActiveSize(sd.maxSegmentSize))
	}

	sp, spErr := spools.New(afero.NewOsFs(), spoolDir, spoolOpts...)
	if spErr != nil {
		logrus.WithError(spErr).Panic("failed to create spool")
	}
	sd.spool = sp

	sd.startTimer()

	return sd
}

// startTimer starts the periodic flush cycle timer (if interval > 0).
func (sd *SpoolDriver) startTimer() {
	if sd.sealCheckInterval == 0 {
		return
	}

	sd.wg.Add(1)
	go func() {
		defer sd.wg.Done()
		logrus.WithField("interval", sd.sealCheckInterval).Debug("started flush cycle timer")

		tk := sd.newTicker(sd.sealCheckInterval)
		defer tk.Stop()

		for {
			select {
			case <-tk.C():
				if err := sd.runFlush(sd.ctx); err != nil {
					logrus.WithError(err).Error("automatic flush cycle failed")
				}
			case <-sd.ctx.Done():
				return
			case <-sd.stopCh:
				return
			}
		}
	}()
}

// Write serializes rows into a self-describing spool frame and appends it to
// the underlying spool keyed by escaped-table + schema-fingerprint.
func (sd *SpoolDriver) Write(_ context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName(table)
	key := streamKey(tableEsc, fingerprint)

	frame := &spoolFrame{
		TableEsc:    tableEsc,
		Fingerprint: fingerprint,
		Schema:      serializeFields(schema),
		Rows:        rows,
	}

	payload, err := encodeFrame(frame)
	if err != nil {
		return fmt.Errorf("encoding warehouse frame: %w", err)
	}

	if err := sd.spool.Append(key, payload); err != nil {
		return fmt.Errorf("appending to spool: %w", err)
	}

	// Cache schema for potential in-memory lookups during flush.
	sd.schemasMu.Lock()
	sd.schemas[key] = schema
	sd.schemasMu.Unlock()

	logrus.WithFields(logrus.Fields{
		"table":       table,
		"fingerprint": fingerprint,
		"row_count":   len(rows),
	}).Debug("wrote rows to spool")

	return nil
}

func streamKey(tableEsc, fingerprint string) string {
	return fmt.Sprintf("%s/%s", tableEsc, fingerprint)
}

// CreateTable is a no-op for spool drivers.
func (sd *SpoolDriver) CreateTable(_ string, _ *arrow.Schema) error {
	return nil
}

// AddColumn is a no-op for spool drivers.
func (sd *SpoolDriver) AddColumn(_ string, _ *arrow.Field) error {
	return nil
}

// MissingColumns always returns an empty slice for spool drivers.
func (sd *SpoolDriver) MissingColumns(_ string, _ *arrow.Schema) ([]*arrow.Field, error) {
	return []*arrow.Field{}, nil
}

// runFlush triggers a spool flush, encoding each inflight file into a
// formatted output and uploading it.
func (sd *SpoolDriver) runFlush(ctx context.Context) error {
	return sd.spool.Flush(func(key string, next func() ([][]byte, error)) error {
		return sd.handleFlush(ctx, key, next)
	})
}

// handleFlush processes one inflight spool file: decodes all frames,
// aggregates rows by (tableEsc, fingerprint), and produces one formatted
// upload per inflight file.
func (sd *SpoolDriver) handleFlush(ctx context.Context, _ string, next func() ([][]byte, error)) error {
	var allRows []map[string]any
	var tableEsc, fingerprint string
	var schema *arrow.Schema

	for {
		batch, err := next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading spool batch: %w", err)
		}
		for _, payload := range batch {
			frame, decErr := decodeFrame(payload)
			if decErr != nil {
				return fmt.Errorf("decoding spool frame: %w", decErr)
			}
			// Use metadata from the first frame; all frames in one key share
			// the same table+fingerprint.
			if schema == nil {
				tableEsc = frame.TableEsc
				fingerprint = frame.Fingerprint
				schema = rebuildSchema(frame.Schema)
			}
			allRows = append(allRows, frame.Rows...)
		}
	}

	if len(allRows) == 0 || schema == nil {
		return nil
	}

	return sd.uploadFormatted(ctx, tableEsc, fingerprint, schema, allRows)
}

// uploadFormatted writes rows through the Format encoder into a temp file
// and uploads it via the Uploader.
func (sd *SpoolDriver) uploadFormatted(
	ctx context.Context,
	tableEsc, fingerprint string,
	schema *arrow.Schema,
	rows []map[string]any,
) error {
	now := time.Now().UTC()
	segmentID := segmentIDFromSealTime(now)

	remoteKey, err := segmentRemoteKey(sd.pathTemplate, tableEsc, fingerprint, segmentID, sd.ext, now)
	if err != nil {
		return fmt.Errorf("generating remote key: %w", err)
	}

	// Write to a temp file in the spool directory so that filesystem
	// uploaders can use rename instead of copy.
	tmpFile, err := os.CreateTemp(sd.spoolDir, "upload-*."+sd.ext)
	if err != nil {
		return fmt.Errorf("creating temp upload file: %w", err)
	}
	tmpPath := tmpFile.Name()

	writeErr := sd.format.Write(tmpFile, schema, rows)
	closeErr := tmpFile.Close()
	if writeErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("writing formatted output: %w", writeErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("closing temp file: %w", closeErr)
	}

	uploadErr := sd.uploader.Upload(ctx, tmpPath, remoteKey)
	if uploadErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("uploading segment: %w", uploadErr)
	}

	// The uploader may have already removed the file (e.g. filesystem
	// uploader uses rename). Remove only if still present.
	_ = os.Remove(tmpPath)

	logrus.WithFields(logrus.Fields{
		"remote_key": remoteKey,
		"rows":       len(rows),
	}).Info("uploaded segment")

	return nil
}

// Close gracefully shuts down the driver.
func (sd *SpoolDriver) Close() error {
	sd.stopOnce.Do(func() { close(sd.stopCh) })
	sd.wg.Wait()
	if sd.flushOnClose {
		if err := sd.runFlush(context.Background()); err != nil {
			return err
		}
	}

	if err := sd.spool.Close(); err != nil {
		return fmt.Errorf("closing spool: %w", err)
	}

	logrus.Info("driver closed")
	return nil
}
