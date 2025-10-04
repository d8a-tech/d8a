package currency

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// FawazAhmedConverter implements Converter using the fawazahmed0/exchange-api dataset.
// It maintains an in-memory cache of base-currency rate tables, fetched initially
// for a provided set of currencies, then refreshed hourly in the background.
// On-demand, when a conversion is requested for a base currency not yet cached,
// the converter will fetch and cache that base table before serving the request.
type FawazAhmedConverter struct {
	httpClient   *http.Client
	mu           sync.RWMutex
	cache        map[string]cachedRates // key: base currency (lower-case ISO code)
	refreshEvery time.Duration
}

type cachedRates struct {
	rates         map[string]float64 // key: quote currency (lower-case ISO code)
	lastRefreshed time.Time
}

const (
	jsDelivrBase   = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies"
	cloudflareBase = "https://latest.currency-api.pages.dev/v1/currencies"
)

// NewFawazAhmedConverter creates a new converter backed by fawazahmed0/exchange-api.
// The constructor synchronously fetches all provided base currencies and blocks until
// they are downloaded and cached. A background refresher updates all cached bases
// every hour.
func NewFawazAhmedConverter(initialBases []string) (Converter, error) {
	if initialBases == nil {
		initialBases = defaultCurrencies()
	}
	// returning interface Converter is intended API for this constructor
	client := &http.Client{Timeout: 15 * time.Second}
	c := &FawazAhmedConverter{
		httpClient:   client,
		cache:        make(map[string]cachedRates),
		refreshEvery: time.Hour,
	}

	// Synchronous initial fetch for all provided base currencies.
	for _, b := range initialBases {
		base := strings.ToLower(strings.TrimSpace(b))
		if base == "" {
			continue
		}
		if err := c.fetchAndCache(base); err != nil {
			return nil, fmt.Errorf("fetch base %s: %w", b, err)
		}
	}

	// Start background refresher.
	go c.backgroundRefresh()

	return c, nil
}

// Convert implements Converter.Convert.
func (c *FawazAhmedConverter) Convert(isoBaseCurrency, isoQuoteCurrency string, amount float64) (float64, error) {
	base := strings.ToLower(strings.TrimSpace(isoBaseCurrency))
	quote := strings.ToLower(strings.TrimSpace(isoQuoteCurrency))
	if base == "" || quote == "" {
		return 0, errors.New("base and quote currencies must be non-empty")
	}
	if base == quote {
		return amount, nil
	}

	// Fast path: try from cache.
	c.mu.RLock()
	entry := c.cache[base]
	rate, okRate := entry.rates[quote]
	c.mu.RUnlock()
	if entry.rates == nil {
		// Fetch on-demand and cache the base table, then retry.
		if err := c.fetchAndCache(base); err != nil {
			return 0, fmt.Errorf("on-demand fetch for %s failed: %w", base, err)
		}
		c.mu.RLock()
		entry = c.cache[base]
		rate, okRate = entry.rates[quote]
		c.mu.RUnlock()
	}

	if !okRate {
		return 0, fmt.Errorf("rate %s/%s not found", isoBaseCurrency, isoQuoteCurrency)
	}

	return amount * rate, nil
}

func (c *FawazAhmedConverter) backgroundRefresh() {
	ticker := time.NewTicker(c.refreshEvery)
	defer ticker.Stop()
	for range ticker.C {
		bases := c.snapshotBases()
		for _, base := range bases {
			_ = c.fetchAndCache(base) // best-effort; errors are ignored in the background
		}
	}
}

func (c *FawazAhmedConverter) snapshotBases() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	bases := make([]string, 0, len(c.cache))
	for base := range c.cache {
		bases = append(bases, base)
	}
	return bases
}

func (c *FawazAhmedConverter) fetchAndCache(base string) error {
	rates, err := c.fetchRates(base)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.cache[base] = cachedRates{rates: rates, lastRefreshed: time.Now()}
	c.mu.Unlock()
	return nil
}

func (c *FawazAhmedConverter) fetchRates(base string) (map[string]float64, error) {
	urls := []string{
		fmt.Sprintf("%s/%s.json", jsDelivrBase, base),
		fmt.Sprintf("%s/%s.json", cloudflareBase, base),
	}
	var lastErr error
	for _, u := range urls {
		m, err := c.tryFetch(u, base)
		if err == nil {
			return m, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("failed to fetch rates")
	}
	return nil, lastErr
}

func (c *FawazAhmedConverter) tryFetch(url, base string) (map[string]float64, error) {
	logrus.Debugf("fetching rates for currency %s from %s", base, url)
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logrus.Errorf("currency: closing response body failed: %v", cerr)
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
	for k, v := range baseObj {
		// Values are numbers; ensure float64
		val, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("malformed payload: rate %s is not a float64", k)
		}
		rates[strings.ToLower(k)] = val
	}

	if len(rates) == 0 {
		return nil, errors.New("no rates in payload")
	}
	return rates, nil
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
