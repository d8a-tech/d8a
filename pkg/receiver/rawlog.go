// Package receiver provides raw log storage implementations for storing and batching raw log data.
package receiver

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/storage"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

//go:embed templates/*.html
var templateFS embed.FS

// RawLogStorage defines the interface for storing raw log data
type RawLogStorage interface {
	Store(*bytes.Buffer) error
}

// RawLogItem represents a single raw log item with its identifier and timestamp
type RawLogItem struct {
	ID            string
	Timestamp     time.Time
	FormattedTime string
}

// FormatTimestamp formats a timestamp to a readable ISO format
func FormatTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05 UTC")
}

// RawLogReader defines the interface for reading raw log data
type RawLogReader interface {
	// ListItems returns a list of raw log items sorted by timestamp (newest first)
	ListItems() ([]RawLogItem, error)
	// GetContent returns the content of a specific raw log item by ID
	GetContent(itemID string) ([]byte, error)
}

// parseTimestampBytesToUnixNano converts the stored ISO8601 timestamp bytes
// (format: 2006-01-02T15:04:05.000000000Z07:00) back to Unix nanoseconds.
func parseTimestampBytesToUnixNano(b []byte) (int64, error) {
	const layout = "2006-01-02T15:04:05.000000000Z07:00"
	t, err := time.Parse(layout, string(b))
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}

// BatchingRawlogStorage is a storage that batches raw log buffers and flushes them to a child storage.
type BatchingRawlogStorage struct {
	mu           sync.Mutex
	currentBatch *bytes.Buffer
	child        RawLogStorage
	batchSize    int
	timeout      time.Duration
	flushTicker  *time.Ticker
	lastFlush    time.Time
	done         chan struct{}
}

// NewBatchingRawlogStorage creates a new BatchingRawlogStorage instance.
func NewBatchingRawlogStorage(child RawLogStorage, batchSize int, timeout time.Duration) *BatchingRawlogStorage {
	brs := &BatchingRawlogStorage{
		child:        child,
		batchSize:    batchSize,
		timeout:      timeout,
		currentBatch: bytes.NewBuffer(nil),
		done:         make(chan struct{}),
	}
	brs.flushTicker = time.NewTicker(timeout)
	brs.lastFlush = time.Now()

	go func() {
		for {
			select {
			case <-brs.flushTicker.C:
				if time.Since(brs.lastFlush) >= brs.timeout {
					if err := brs.Flush(); err != nil {
						logrus.Errorf("Failed to flush rawlog batch: %v", err)
					}
				}
			case <-brs.done:
				return
			}
		}
	}()

	return brs
}

// Store implements the RawLogStorage interface
func (brs *BatchingRawlogStorage) Store(buffer *bytes.Buffer) error {
	brs.mu.Lock()
	defer brs.mu.Unlock()

	if brs.currentBatch.Len() == 0 {
		brs.currentBatch = buffer
	} else {
		// Add separator between requests to avoid confusion
		brs.currentBatch.WriteString("\n\n--- REQUEST SEPARATOR ---\n\n")
		brs.currentBatch.Write(buffer.Bytes())
	}

	if brs.currentBatch.Len() >= brs.batchSize {
		return brs.flushLocked()
	}
	return nil
}

// Flush flushes the buffer to the child storage.
func (brs *BatchingRawlogStorage) Flush() error {
	brs.mu.Lock()
	defer brs.mu.Unlock()
	return brs.flushLocked()
}

func (brs *BatchingRawlogStorage) flushLocked() error {
	brs.lastFlush = time.Now()
	if brs.currentBatch.Len() == 0 {
		return nil
	}

	// Process each buffer in the batch
	if err := brs.child.Store(brs.currentBatch); err != nil {
		logrus.Errorf("Rawlog storage failed: %v", err)
		return fmt.Errorf("failed to store rawlog data: %w", err)
	}

	// Clear the buffer
	brs.currentBatch.Reset()
	return nil
}

// Close closes the BatchingRawlogStorage instance.
func (brs *BatchingRawlogStorage) Close() {
	brs.flushTicker.Stop()
	close(brs.done)
	if err := brs.Flush(); err != nil {
		logrus.Errorf("Failed to flush rawlog batch: %v", err)
	}
}

// StorageSetRawLogStorage implements RawLogStorage using a storage.KV backend.
type StorageSetRawLogStorage struct {
	rawlogSet storage.Set
	indexSet  storage.Set
}

var _ RawLogStorage = &StorageSetRawLogStorage{}
var _ RawLogReader = &StorageSetRawLogStorage{}

// NewFromStorageSetRawLogStorage creates a new StorageSetRawLogStorage instance.
func NewFromStorageSetRawLogStorage(rawlogSet, indexSet storage.Set) *StorageSetRawLogStorage {
	return &StorageSetRawLogStorage{
		rawlogSet: rawlogSet,
		indexSet:  indexSet,
	}
}

// Store implements the RawLogStorage interface by storing the buffer data in KV storage.
// It generates a unique timestamp-based key for each log entry.
func (s *StorageSetRawLogStorage) Store(buffer *bytes.Buffer) error {
	// Generate a unique key using ISO8601 timestamp and UUID
	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z07:00")

	// Store the buffer bytes using the generated key
	err := s.rawlogSet.Add([]byte(timestamp), buffer.Bytes())
	if err != nil {
		return fmt.Errorf("failed to store raw log data: %w", err)
	}

	err = s.indexSet.Add([]byte("items"), []byte(timestamp))
	if err != nil {
		return fmt.Errorf("failed to store raw log index: %w", err)
	}

	return nil
}

func (s *StorageSetRawLogStorage) ListItems() ([]RawLogItem, error) {
	items, err := s.indexSet.All([]byte("items"))
	if err != nil {
		return nil, fmt.Errorf("failed to list raw log items: %w", err)
	}
	rawLogItems := make([]RawLogItem, len(items))
	for i, item := range items {
		// Convert stored ISO8601 bytes back to time
		unixNano, parseErr := parseTimestampBytesToUnixNano(item)
		if parseErr != nil {
			// Fallback: keep raw ID and leave time zeroed
			rawLogItems[i] = RawLogItem{
				ID:            string(item),
				Timestamp:     time.Time{},
				FormattedTime: string(item),
			}
			continue
		}
		t := time.Unix(0, unixNano).UTC()
		rawLogItems[i] = RawLogItem{
			ID:            string(item),
			Timestamp:     t,
			FormattedTime: FormatTimestamp(t),
		}
	}
	return rawLogItems, nil
}

func (s *StorageSetRawLogStorage) GetContent(itemID string) ([]byte, error) {
	content, err := s.rawlogSet.All([]byte(itemID))
	if err != nil {
		return nil, fmt.Errorf("failed to get raw log item content: %w", err)
	}
	return bytes.Join(content, []byte("\n")), nil
}

type dummyRawLogStorage struct{}

func (d *dummyRawLogStorage) Store(_ *bytes.Buffer) error {
	return nil
}

// NewDummyRawLogStorage creates a dummy raw log storage that discards all data.
func NewDummyRawLogStorage() RawLogStorage {
	return &dummyRawLogStorage{}
}

// RawLogMainPageHandler returns a handler for the main rawlog page showing all index items
func RawLogMainPageHandler(rawLogIndexSet storage.Set) func(fctx *fasthttp.RequestCtx) {
	tmpl := template.Must(template.ParseFS(templateFS, "templates/main.html"))

	return func(fctx *fasthttp.RequestCtx) {
		items, err := rawLogIndexSet.All([]byte("items"))
		if err != nil {
			fctx.SetStatusCode(fasthttp.StatusInternalServerError)
			if _, writeErr := fctx.WriteString("Error loading items"); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		// Convert byte slices to strings for template
		itemStrings := make([]string, len(items))
		for i, item := range items {
			itemStrings[i] = string(item)
		}

		data := struct {
			Items []string
		}{
			Items: itemStrings,
		}

		fctx.Response.Header.Set("Content-Type", "text/html")

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			fctx.SetStatusCode(fasthttp.StatusInternalServerError)
			if _, writeErr := fctx.WriteString("Error rendering template"); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		if _, writeErr := fctx.Write(buf.Bytes()); writeErr != nil {
			logrus.Errorf("Failed to write response: %v", writeErr)
		}
	}
}

// RawLogMainPageHandlerFromReader returns a handler for the main rawlog page using RawLogReader
func RawLogMainPageHandlerFromReader(reader RawLogReader) func(fctx *fasthttp.RequestCtx) {
	tmpl := template.Must(template.ParseFS(templateFS, "templates/main.html"))

	return func(fctx *fasthttp.RequestCtx) {
		items, err := reader.ListItems()
		if err != nil {
			fctx.SetStatusCode(fasthttp.StatusInternalServerError)
			if _, writeErr := fctx.WriteString("Error loading items"); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		data := struct {
			Items []RawLogItem
		}{
			Items: items,
		}

		fctx.Response.Header.Set("Content-Type", "text/html")

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			fctx.SetStatusCode(fasthttp.StatusInternalServerError)
			if _, writeErr := fctx.WriteString("Error rendering template"); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		if _, writeErr := fctx.Write(buf.Bytes()); writeErr != nil {
			logrus.Errorf("Failed to write response: %v", writeErr)
		}
	}
}

// RawLogDetailPageHandler returns a handler for the detail page showing content of a specific item
func RawLogDetailPageHandler(rawLogSet storage.Set) func(fctx *fasthttp.RequestCtx) {
	tmpl := template.Must(template.ParseFS(templateFS, "templates/detail.html"))

	return func(fctx *fasthttp.RequestCtx) {
		path := string(fctx.Path())
		// Extract item ID from path like "/rawlog/1234567890_uuid"
		parts := strings.Split(path, "/")
		if len(parts) < 3 || parts[2] == "" {
			fctx.SetStatusCode(fasthttp.StatusBadRequest)
			if _, writeErr := fctx.WriteString("Invalid path"); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		itemID := parts[2]

		// Get the content for this item from rawLogSet
		content, err := rawLogSet.All([]byte(itemID))
		if err != nil {
			fctx.SetStatusCode(fasthttp.StatusInternalServerError)
			if _, writeErr := fctx.WriteString("Error loading item content"); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		// Combine all content pieces
		var contentStr strings.Builder
		for _, piece := range content {
			contentStr.Write(piece)
		}

		data := struct {
			ItemID  string
			Content string
		}{
			ItemID:  itemID,
			Content: contentStr.String(),
		}

		fctx.Response.Header.Set("Content-Type", "text/html")

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			fctx.SetStatusCode(fasthttp.StatusInternalServerError)
			if _, writeErr := fctx.WriteString("Error rendering template"); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		if _, writeErr := fctx.Write(buf.Bytes()); writeErr != nil {
			logrus.Errorf("Failed to write response: %v", writeErr)
		}
	}
}

// RawLogDetailPageHandlerFromReader returns a handler for the detail page using RawLogReader
func RawLogDetailPageHandlerFromReader(reader RawLogReader) func(fctx *fasthttp.RequestCtx) {
	tmpl := template.Must(template.ParseFS(templateFS, "templates/detail.html"))

	return func(fctx *fasthttp.RequestCtx) {
		path := string(fctx.Path())
		// Extract item ID from path like "/rawlog/1234567890"
		parts := strings.Split(path, "/")
		if len(parts) < 3 || parts[2] == "" {
			fctx.SetStatusCode(fasthttp.StatusBadRequest)
			if _, writeErr := fctx.WriteString("Invalid path"); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		itemID := parts[2]

		// Get the content for this item from reader
		content, err := reader.GetContent(itemID)
		if err != nil {
			fctx.SetStatusCode(fasthttp.StatusInternalServerError)
			if _, writeErr := fmt.Fprintf(fctx, "Error loading item content: %v", err); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		// Parse timestamp from itemID to create formatted time
		var formattedTime string
		if timestamp, parseErr := strconv.ParseInt(itemID, 10, 64); parseErr == nil {
			formattedTime = FormatTimestamp(time.Unix(0, timestamp))
		} else {
			formattedTime = itemID // fallback to raw ID if parsing fails
		}

		data := struct {
			ItemID        string
			FormattedTime string
			Content       string
		}{
			ItemID:        itemID,
			FormattedTime: formattedTime,
			Content:       string(content),
		}

		fctx.Response.Header.Set("Content-Type", "text/html")

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			fctx.SetStatusCode(fasthttp.StatusInternalServerError)
			if _, writeErr := fctx.WriteString("Error rendering template"); writeErr != nil {
				logrus.Errorf("Failed to write error response: %v", writeErr)
			}
			return
		}

		if _, writeErr := fctx.Write(buf.Bytes()); writeErr != nil {
			logrus.Errorf("Failed to write response: %v", writeErr)
		}
	}
}
