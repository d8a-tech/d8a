package files

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/template"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/spools"
	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/d8a-tech/d8a/pkg/warehouse"
)

// FilesOption is a functional option for configuring FilesDriver.
type FilesOption func(*FilesDriver)

// WithPathTemplate sets the path template string for remote object keys.
func WithPathTemplate(tmplStr string) FilesOption {
	return func(sd *FilesDriver) {
		sd.pathTemplateStr = tmplStr
	}
}

// FilesDriver writes warehouse rows into a keyed spool and flushes them to remote storage.
type FilesDriver struct {
	spool           spools.Spool
	kv              storage.KV
	uploader        StreamUploader
	format          Format
	ext             string
	pathTemplate    *template.Template
	pathTemplateStr string
}

var _ warehouse.Driver = (*FilesDriver)(nil)

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

func NewFilesDriver(
	_ context.Context,
	spoolFactory spools.Factory,
	kv storage.KV,
	uploader StreamUploader,
	format Format,
	opts ...FilesOption,
) (*FilesDriver, error) {
	sd := &FilesDriver{
		kv:       kv,
		uploader: uploader,
		format:   format,
		ext:      format.Extension(),
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
		return nil, fmt.Errorf("parsing path template: %w", err)
	}
	sd.pathTemplate = tmpl

	handler := buildFlushHandler(sd)

	spool, err := spoolFactory.Create(handler)
	if err != nil {
		return nil, fmt.Errorf("creating spool: %w", err)
	}
	sd.spool = spool

	return sd, nil
}

//nolint:contextcheck // flush handler signature has no context; use non-canceled context per invocation.
func buildFlushHandler(sd *FilesDriver) spools.FlushHandler {
	return func(key string, next func() ([][]byte, error)) error {
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

		upload, err := sd.uploader.Begin(context.Background(), remoteKey)
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
	}
}

func (sd *FilesDriver) Write(ctx context.Context, table string, schema *arrow.Schema, rows []map[string]any) error {
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
func (sd *FilesDriver) CreateTable(table string, schema *arrow.Schema) error {
	return nil
}

// AddColumn is a no-op for spool drivers.
func (sd *FilesDriver) AddColumn(table string, field *arrow.Field) error {
	return nil
}

// MissingColumns always returns an empty slice for spool drivers.
func (sd *FilesDriver) MissingColumns(table string, schema *arrow.Schema) ([]*arrow.Field, error) {
	return []*arrow.Field{}, nil
}

// Close gracefully shuts down the driver.
func (sd *FilesDriver) Close() error {
	if c, ok := sd.kv.(interface{ Close() error }); ok {
		return c.Close()
	}

	return nil
}
