package files

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/warehouse"
	"github.com/sirupsen/logrus"
)

// SpoolOption is a functional option for configuring SpoolDriver.
type SpoolOption func(*SpoolDriver)

// withManualCycle disables automatic timer-based flush cycles (test-only).
func withManualCycle() SpoolOption {
	return func(sd *SpoolDriver) {
		sd.flushInterval = 0
	}
}

// WithFlushInterval sets how often periodic flushes run.
func WithFlushInterval(d time.Duration) SpoolOption {
	return func(sd *SpoolDriver) {
		sd.flushInterval = d
	}
}

// WithFlushOnClose configures whether Close flushes all pending frames.
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

type ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct {
	t *time.Ticker
}

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

// SpoolDriver writes warehouse rows into a keyed spool and flushes them to remote storage.
type SpoolDriver struct {
	ctx             context.Context
	spool           spools.Spool
	kv              storage.KV
	uploader        StreamUploader
	format          Format
	ext             string
	pathTemplate    *template.Template
	pathTemplateStr string
	flushInterval   time.Duration
	stopCh          chan struct{}
	stopOnce        sync.Once
	wg              sync.WaitGroup
	newTicker       func(time.Duration) ticker
	flushOnClose    bool
}

var _ warehouse.Driver = (*SpoolDriver)(nil)

// parsePathTemplate parses and validates a path template string.
func parsePathTemplate(tmplStr string) (*template.Template, error) {
	tmplStr = strings.TrimSpace(tmplStr)
	if tmplStr == "" {
		return nil, fmt.Errorf("template string cannot be empty")
	}

	tmpl, err := template.New("path").Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	sampleData := pathTemplateData{
		Table:       "test",
		Schema:      "abc123",
		SegmentID:   "12345_uuid",
		Extension:   "csv",
		Year:        2026,
		Month:       3,
		MonthPadded: "03",
		Day:         1,
		DayPadded:   "01",
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, sampleData); err != nil {
		return nil, fmt.Errorf("executing template with sample data: %w", err)
	}

	if strings.Contains(buf.String(), "..") {
		return nil, fmt.Errorf("template output contains path traversal sequence (..)")
	}

	return tmpl, nil
}

func NewSpoolDriver(
	ctx context.Context,
	spool spools.Spool,
	kv storage.KV,
	uploader StreamUploader,
	format Format,
	opts ...SpoolOption,
) *SpoolDriver {
	sd := &SpoolDriver{
		ctx:           ctx,
		spool:         spool,
		kv:            kv,
		uploader:      uploader,
		format:        format,
		ext:           format.Extension(),
		flushInterval: 15 * time.Second,
		stopCh:        make(chan struct{}),
	}

	sd.newTicker = func(d time.Duration) ticker {
		return &realTicker{t: time.NewTicker(d)}
	}

	for _, opt := range opts {
		opt(sd)
	}

	tmplStr := sd.pathTemplateStr
	if tmplStr == "" {
		tmplStr = "table={{.Table}}/schema={{.Schema}}/dt={{.Year}}/{{.MonthPadded}}/{{.DayPadded}}/" +
			"{{.SegmentID}}.{{.Extension}}"
	}

	tmpl, err := parsePathTemplate(tmplStr)
	if err != nil {
		logrus.WithError(err).Panic("failed to parse path template")
	}
	sd.pathTemplate = tmpl

	sd.startTimer()

	return sd
}

func (sd *SpoolDriver) startTimer() {
	if sd.flushInterval <= 0 {
		return
	}

	sd.wg.Add(1)
	go func() {
		defer sd.wg.Done()

		tk := sd.newTicker(sd.flushInterval)
		defer tk.Stop()

		for {
			select {
			case <-tk.C():
				if err := sd.flushOnce(sd.ctx); err != nil {
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

func (sd *SpoolDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
	_ = ctx

	fingerprint := schemaFingerprint(schema)
	tableEsc := escapeTableName(table)
	key := tableEsc + "/" + fingerprint

	schemaData, err := marshalSchema(schema)
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}

	if _, err := sd.kv.Set(
		[]byte(fingerprint),
		schemaData,
		storage.WithSkipIfKeyAlreadyExists(true),
	); err != nil {
		return fmt.Errorf("storing schema metadata for fingerprint %s: %w", fingerprint, err)
	}

	payload, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("marshaling rows payload: %w", err)
	}

	if err := sd.spool.Append(key, payload); err != nil {
		return fmt.Errorf("appending rows to spool: %w", err)
	}

	return nil
}

func (sd *SpoolDriver) flushOnce(ctx context.Context) error {
	return sd.spool.Flush(func(key string, next func() ([][]byte, error)) error {
		tableEsc, fingerprint, err := parseSpoolKey(key)
		if err != nil {
			return err
		}

		schemaData, err := sd.kv.Get([]byte(fingerprint))
		if err != nil {
			return fmt.Errorf("getting schema metadata for fingerprint %s: %w", fingerprint, err)
		}
		if len(schemaData) == 0 {
			return fmt.Errorf("missing schema metadata for fingerprint %s", fingerprint)
		}

		schema, err := unmarshalSchema(schemaData)
		if err != nil {
			return fmt.Errorf("unmarshaling schema for fingerprint %s: %w", fingerprint, err)
		}

		now := time.Now().UTC()
		segmentID := segmentIDFromSealTime(now)
		remoteKey, err := segmentRemoteKey(sd.pathTemplate, tableEsc, fingerprint, segmentID, sd.ext, now)
		if err != nil {
			return fmt.Errorf("building remote key for %q: %w", key, err)
		}

		upload, err := sd.uploader.Begin(ctx, remoteKey)
		if err != nil {
			return fmt.Errorf("beginning upload for key %s: %w", remoteKey, err)
		}

		abortWith := func(cause error) error {
			abortErr := upload.Abort()
			if abortErr != nil {
				return errors.Join(cause, fmt.Errorf("aborting upload: %w", abortErr))
			}
			return cause
		}

		fw, err := sd.format.NewWriter(upload.Writer(), schema)
		if err != nil {
			return abortWith(fmt.Errorf("creating format writer: %w", err))
		}

		for {
			frames, err := next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return abortWith(fmt.Errorf("reading spool frames: %w", err))
			}

			for _, frame := range frames {
				var decodedRows []map[string]any
				if err := json.Unmarshal(frame, &decodedRows); err != nil {
					return abortWith(fmt.Errorf("decoding rows payload: %w", err))
				}

				if err := fw.WriteRows(decodedRows); err != nil {
					return abortWith(fmt.Errorf("writing rows to format writer: %w", err))
				}
			}
		}

		if err := fw.Close(); err != nil {
			return abortWith(fmt.Errorf("closing format writer: %w", err))
		}

		if err := upload.Commit(); err != nil {
			return abortWith(fmt.Errorf("committing upload: %w", err))
		}

		return nil
	})
}

func parseSpoolKey(key string) (tableEsc, fingerprint string, err error) {
	if strings.Contains(key, "/") {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			return parts[0], parts[1], nil
		}
	}

	idx := strings.LastIndexByte(key, '_')
	if idx <= 0 || idx+1 >= len(key) {
		return "", "", fmt.Errorf("invalid spool key %q", key)
	}

	tableEsc = key[:idx]
	fingerprint = key[idx+1:]
	if len(fingerprint) != 16 {
		return "", "", fmt.Errorf("invalid spool key %q", key)
	}

	return tableEsc, fingerprint, nil
}

// CreateTable is a no-op for spool drivers.
func (sd *SpoolDriver) CreateTable(table string, schema *arrow.Schema) error {
	return nil
}

// AddColumn is a no-op for spool drivers.
func (sd *SpoolDriver) AddColumn(table string, field *arrow.Field) error {
	return nil
}

// MissingColumns always returns an empty slice for spool drivers.
func (sd *SpoolDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	return []*arrow.Field{}, nil
}

// Close gracefully shuts down the driver.
func (sd *SpoolDriver) Close() error {
	sd.stopOnce.Do(func() { close(sd.stopCh) })
	sd.wg.Wait()

	var flushErr error
	if sd.flushOnClose {
		flushErr = sd.flushOnce(context.Background())
	}

	closeErr := sd.spool.Close()

	var kvCloseErr error
	if c, ok := sd.kv.(interface{ Close() error }); ok {
		kvCloseErr = c.Close()
	}

	if flushErr != nil || closeErr != nil || kvCloseErr != nil {
		return errors.Join(flushErr, closeErr, kvCloseErr)
	}

	return nil
}
