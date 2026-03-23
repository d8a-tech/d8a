package currency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	defaultDestinationDir = "./currency"
	defaultRefreshEvery   = 6 * time.Hour
	jsDelivrBase          = "https://cdn.jsdelivr.net/npm/@FWA0/currency-api@latest/v1/currencies"
	cloudflareBase        = "https://latest.currency-api.pages.dev/v1/currencies"
)

const IntervalNever = time.Duration(0)

var ErrNoSnapshot = errors.New("currency snapshot not found")

type Downloader interface {
	Download(ctx context.Context) (*Snapshot, error)
}

type SnapshotStore interface {
	Latest() (*Snapshot, error)
	Append(snapshot *Snapshot) error
}

type Snapshot struct {
	CreatedAt time.Time                     `json:"created_at"`
	Rates     map[string]map[string]float64 `json:"rates"`
}

type converterState struct {
	snapshot *Snapshot
	rates    map[string]map[string]float64
}

type FWAConverter struct {
	downloader     Downloader
	store          SnapshotStore
	refreshEvery   time.Duration
	refreshCounter metric.Int64Counter
	state          atomic.Pointer[converterState]
	runOnce        sync.Once
}

type fwaOption func(*FWAConverter)

type apiDownloader struct {
	httpClient *http.Client
	bases      []string
}

type fileStore struct {
	directory string
	mu        sync.Mutex
}

func init() {
	meter := otel.GetMeterProvider().Meter("currency")
	refreshCounter, _ := meter.Int64Counter(
		"currency.refresh_attempts",
		metric.WithDescription("Currency refresh attempts grouped by result"),
	)
	defaultRefreshAttemptsCounter = refreshCounter
}

var defaultRefreshAttemptsCounter metric.Int64Counter

func WithDownloader(downloader Downloader) fwaOption {
	return func(c *FWAConverter) {
		c.downloader = downloader
	}
}

func WithStore(store SnapshotStore) fwaOption {
	return func(c *FWAConverter) {
		c.store = store
	}
}

func WithDestination(directory string) fwaOption {
	return func(c *FWAConverter) {
		c.store = NewFileStore(directory)
	}
}

func WithInterval(interval time.Duration) fwaOption {
	return func(c *FWAConverter) {
		c.refreshEvery = interval
	}
}

func NewAPIDownloader() Downloader {
	return &apiDownloader{
		httpClient: &http.Client{Timeout: 15 * time.Second},
		bases:      defaultCurrencies(),
	}
}

func NewFileStore(directory string) SnapshotStore {
	return &fileStore{directory: directory}
}

// NewFWAConverter creates a new converter backed by persisted currency snapshots.

func NewFWAConverter(initialBases []string, options ...fwaOption) (ManagedConverter, error) {
	bases := initialBases
	if bases == nil {
		bases = defaultCurrencies()
	}

	converter := &FWAConverter{
		downloader: &apiDownloader{
			httpClient: &http.Client{Timeout: 15 * time.Second},
			bases:      append([]string(nil), bases...),
		},
		store:          NewFileStore(defaultDestinationDir),
		refreshEvery:   defaultRefreshEvery,
		refreshCounter: defaultRefreshAttemptsCounter,
	}

	for _, option := range options {
		option(converter)
	}

	snapshot, err := converter.store.Latest()
	if err != nil {
		if errors.Is(err, ErrNoSnapshot) {
			return converter, nil
		}
		return nil, fmt.Errorf("load latest currency snapshot: %w", err)
	}

	converter.setSnapshot(snapshot)
	return converter, nil
}

// Convert implements Converter.Convert.
func (c *FWAConverter) Convert(isoBaseCurrency, isoQuoteCurrency string, amount float64) (float64, error) {
	base := strings.ToLower(strings.TrimSpace(isoBaseCurrency))
	quote := strings.ToLower(strings.TrimSpace(isoQuoteCurrency))
	if base == "" || quote == "" {
		return 0, errors.New("base and quote currencies must be non-empty")
	}
	if base == quote {
		return amount, nil
	}

	state := c.state.Load()
	if state == nil || state.snapshot == nil {
		return 0, ErrUnavailable
	}

	quotes, ok := state.rates[base]
	if !ok {
		return 0, ErrUnavailable
	}
	rate, ok := quotes[quote]
	if !ok {
		return 0, ErrUnavailable
	}

	return amount * rate, nil
}

// Run starts the background refresh loop.
func (c *FWAConverter) Run(ctx context.Context) {
	c.runOnce.Do(func() {
		if c.refreshEvery == IntervalNever {
			return
		}

		go c.refreshLoop(ctx)
	})
}

// HasSnapshot reports whether a snapshot is currently loaded in memory.
func (c *FWAConverter) HasSnapshot() bool {
	state := c.state.Load()
	return state != nil && state.snapshot != nil
}

func (c *FWAConverter) refreshLoop(ctx context.Context) {
	c.refresh(ctx)

	ticker := time.NewTicker(c.refreshEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refresh(ctx)
		}
	}
}

func (c *FWAConverter) refresh(ctx context.Context) {
	latestSnapshot, err := c.store.Latest()
	if err == nil && time.Since(latestSnapshot.CreatedAt) < c.refreshEvery {
		c.setSnapshot(latestSnapshot)
		return
	}
	if err != nil && !errors.Is(err, ErrNoSnapshot) {
		logrus.WithError(err).Warn("currency: failed to inspect local snapshots before refresh")
	}

	snapshot, err := c.downloader.Download(ctx)
	if err != nil {
		c.refreshCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "failure")))
		logrus.WithError(err).Warn("currency: snapshot refresh failed")
		return
	}
	if err := c.store.Append(snapshot); err != nil {
		c.refreshCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "failure")))
		logrus.WithError(err).Warn("currency: failed to persist refreshed snapshot")
		return
	}

	c.setSnapshot(snapshot)
	c.refreshCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "success")))
	logrus.Debug("currency: refreshed rates snapshot")
}

func (c *FWAConverter) setSnapshot(snapshot *Snapshot) {
	normalized := normalizeRates(snapshot.Rates)
	c.state.Store(&converterState{
		snapshot: &Snapshot{
			CreatedAt: snapshot.CreatedAt,
			Rates:     cloneRates(normalized),
		},
		rates: normalized,
	})
}

// Download fetches the current rates snapshot from the remote dataset.
func (d *apiDownloader) Download(ctx context.Context) (*Snapshot, error) {
	rates := make(map[string]map[string]float64, len(d.bases))
	for _, baseCurrency := range d.bases {
		base := strings.ToLower(strings.TrimSpace(baseCurrency))
		if base == "" {
			continue
		}

		baseRates, err := d.fetchRates(ctx, base)
		if err != nil {
			return nil, fmt.Errorf("fetch rates for %s: %w", baseCurrency, err)
		}
		rates[base] = baseRates
	}

	if len(rates) == 0 {
		return nil, errors.New("no currency rates downloaded")
	}

	return &Snapshot{
		CreatedAt: time.Now().UTC(),
		Rates:     rates,
	}, nil
}

func (d *apiDownloader) fetchRates(ctx context.Context, base string) (map[string]float64, error) {
	urls := []string{
		fmt.Sprintf("%s/%s.json", jsDelivrBase, base),
		fmt.Sprintf("%s/%s.json", cloudflareBase, base),
	}
	var lastErr error
	for _, url := range urls {
		rates, err := d.tryFetch(ctx, url, base)
		if err == nil {
			return rates, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("failed to fetch rates")
	}
	return nil, lastErr
}

func (d *apiDownloader) tryFetch(ctx context.Context, url, base string) (map[string]float64, error) {
	logrus.Debugf("currency: fetching rates for %s from %s", base, url)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logrus.WithError(closeErr).Error("currency: closing response body failed")
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	baseObjRaw, ok := payload[base]
	if !ok {
		return nil, fmt.Errorf("base '%s' not present in payload", base)
	}

	baseObj, ok := baseObjRaw.(map[string]any)
	if !ok {
		return nil, errors.New("malformed payload: base object is not a map")
	}

	rates := make(map[string]float64, len(baseObj))
	for quote, rawValue := range baseObj {
		value, ok := rawValue.(float64)
		if !ok {
			return nil, fmt.Errorf("malformed payload: rate %s is not a float64", quote)
		}
		rates[strings.ToLower(quote)] = value
	}

	if len(rates) == 0 {
		return nil, errors.New("no rates in payload")
	}
	return rates, nil
}

// Latest returns the newest stored snapshot.
func (s *fileStore) Latest() (*Snapshot, error) {
	if err := os.MkdirAll(s.directory, 0o750); err != nil {
		return nil, fmt.Errorf("create currency snapshot directory: %w", err)
	}

	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return nil, fmt.Errorf("read currency snapshot directory: %w", err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		files = append(files, filepath.Join(s.directory, entry.Name()))
	}
	if len(files) == 0 {
		return nil, ErrNoSnapshot
	}

	sort.Strings(files)
	for i := len(files) - 1; i >= 0; i-- {
		snapshot, err := readSnapshot(files[i])
		if err == nil {
			return snapshot, nil
		}
		logrus.WithError(err).Warnf("currency: failed to load snapshot %s", files[i])
	}

	return nil, ErrNoSnapshot
}

// Append persists a new immutable snapshot file.
func (s *fileStore) Append(snapshot *Snapshot) error {
	if snapshot == nil {
		return errors.New("snapshot is nil")
	}
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = time.Now().UTC()
	}

	if err := os.MkdirAll(s.directory, 0o750); err != nil {
		return fmt.Errorf("create currency snapshot directory: %w", err)
	}

	payload := &Snapshot{
		CreatedAt: snapshot.CreatedAt.UTC(),
		Rates:     cloneRates(normalizeRates(snapshot.Rates)),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal currency snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fileName := fmt.Sprintf("rates-%s.json", payload.CreatedAt.Format("20060102T150405.000000000Z07:00"))
	path := filepath.Join(s.directory, sanitizeSnapshotFilename(fileName))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write currency snapshot: %w", err)
	}
	return nil
}

func readSnapshot(path string) (*Snapshot, error) {
	// #nosec G304 -- path comes from files enumerated in the configured snapshot directory
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	if snapshot.CreatedAt.IsZero() {
		return nil, errors.New("snapshot created_at is missing")
	}
	if len(snapshot.Rates) == 0 {
		return nil, errors.New("snapshot rates are empty")
	}

	snapshot.CreatedAt = snapshot.CreatedAt.UTC()
	snapshot.Rates = normalizeRates(snapshot.Rates)
	return &snapshot, nil
}

func sanitizeSnapshotFilename(name string) string {
	replacer := strings.NewReplacer(":", "-", "/", "-", "\\", "-")
	return replacer.Replace(name)
}

func normalizeRates(input map[string]map[string]float64) map[string]map[string]float64 {
	normalized := make(map[string]map[string]float64, len(input))
	for base, quotes := range input {
		normalizedBase := strings.ToLower(strings.TrimSpace(base))
		if normalizedBase == "" {
			continue
		}
		normalizedQuotes := make(map[string]float64, len(quotes))
		for quote, value := range quotes {
			normalizedQuote := strings.ToLower(strings.TrimSpace(quote))
			if normalizedQuote == "" {
				continue
			}
			normalizedQuotes[normalizedQuote] = value
		}
		if len(normalizedQuotes) == 0 {
			continue
		}
		normalized[normalizedBase] = normalizedQuotes
	}
	return normalized
}

func cloneRates(input map[string]map[string]float64) map[string]map[string]float64 {
	cloned := make(map[string]map[string]float64, len(input))
	for base, quotes := range input {
		quotesCopy := make(map[string]float64, len(quotes))
		for quote, value := range quotes {
			quotesCopy[quote] = value
		}
		cloned[base] = quotesCopy
	}
	return cloned
}

func defaultCurrencies() []string {
	return []string{
		ISOCurrencyUSD,
		ISOCurrencyEUR,
		ISOCurrencyGBP,
		ISOCurrencyJPY,
		ISOCurrencyCAD,
		ISOCurrencyCHF,
		ISOCurrencyCNY,
	}
}
