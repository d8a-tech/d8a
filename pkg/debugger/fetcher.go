// nolint // this is debugger code, not for prod and 100% vibe-coded
package debugger

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/d8a-tech/d8a/pkg/receiver"
	"github.com/sirupsen/logrus"
)

// RemoteFetcher implements RawLogReader interface for fetching rawlog data from remote HTTP endpoints
type RemoteFetcher struct {
	baseURL    string
	httpClient *http.Client
	cache      *fetcherCache
}

// fetcherCache provides simple in-memory caching for fetched data
type fetcherCache struct {
	mu          sync.RWMutex
	items       []receiver.RawLogItem
	itemsExpiry time.Time
	content     map[string][]byte
	contentTTL  time.Duration
	itemsTTL    time.Duration
}

// NewRemoteFetcher creates a new RemoteFetcher instance
func NewRemoteFetcher(baseURL string) *RemoteFetcher {
	// Ensure baseURL doesn't have trailing slash
	baseURL = strings.TrimRight(baseURL, "/")

	return &RemoteFetcher{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		cache: &fetcherCache{
			content:    make(map[string][]byte),
			contentTTL: 5 * time.Minute, // Cache content for 5 minutes
			itemsTTL:   1 * time.Minute, // Cache items list for 1 minute
		},
	}
}

// ListItems implements receiver.RawLogReader interface
func (f *RemoteFetcher) ListItems() ([]receiver.RawLogItem, error) {
	// Check cache first
	f.cache.mu.RLock()
	if time.Now().Before(f.cache.itemsExpiry) && len(f.cache.items) > 0 {
		items := make([]receiver.RawLogItem, len(f.cache.items))
		copy(items, f.cache.items)
		f.cache.mu.RUnlock()
		logrus.Debug("Returning cached rawlog items")
		return items, nil
	}
	f.cache.mu.RUnlock()

	// Fetch from remote
	items, err := f.fetchRawlogIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rawlog index: %w", err)
	}

	// Update cache
	f.cache.mu.Lock()
	f.cache.items = items
	f.cache.itemsExpiry = time.Now().Add(f.cache.itemsTTL)
	f.cache.mu.Unlock()

	logrus.Debugf("Fetched %d rawlog items from remote", len(items))
	return items, nil
}

// GetContent implements receiver.RawLogReader interface
func (f *RemoteFetcher) GetContent(itemID string) ([]byte, error) {
	// Check cache first
	f.cache.mu.RLock()
	if content, exists := f.cache.content[itemID]; exists {
		// Create a copy to avoid data races
		contentCopy := make([]byte, len(content))
		copy(contentCopy, content)
		f.cache.mu.RUnlock()
		logrus.Debugf("Returning cached content for item %s", itemID)
		return contentCopy, nil
	}
	f.cache.mu.RUnlock()

	// Fetch from remote
	content, err := f.fetchRawlogContent(itemID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rawlog content for item %s: %w", itemID, err)
	}

	// Update cache
	f.cache.mu.Lock()
	f.cache.content[itemID] = content
	// Clean up old cache entries to prevent memory leaks
	f.cleanupContentCache()
	f.cache.mu.Unlock()

	logrus.Debugf("Fetched content for item %s (%d bytes)", itemID, len(content))
	return content, nil
}

// fetchRawlogIndex fetches the rawlog index page and parses it for rawlog items
func (f *RemoteFetcher) fetchRawlogIndex() ([]receiver.RawLogItem, error) {
	indexURL := f.baseURL + "/rawlogs"

	resp, err := f.httpClient.Get(indexURL)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.Errorf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status %d: %s", resp.StatusCode, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	items, err := f.parseRawlogIndexHTML(string(body))
	if err != nil {
		return nil, fmt.Errorf("failed to parse rawlog index HTML: %w", err)
	}

	return items, nil
}

// fetchRawlogContent fetches the content of a specific rawlog item
func (f *RemoteFetcher) fetchRawlogContent(itemID string) ([]byte, error) {
	// Validate itemID format (should be a timestamp)
	if _, err := strconv.ParseInt(itemID, 10, 64); err != nil {
		return nil, fmt.Errorf("invalid item ID format: %w", err)
	}

	contentURL := f.baseURL + "/rawlog/" + url.PathEscape(itemID)

	resp, err := f.httpClient.Get(contentURL)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.Errorf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("rawlog item %s not found", itemID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status %d: %s", resp.StatusCode, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse HTML to extract raw log content
	content, err := f.parseRawlogContentHTML(string(body))
	if err != nil {
		return nil, fmt.Errorf("failed to parse rawlog content HTML: %w", err)
	}

	return []byte(content), nil
}

// parseRawlogIndexHTML parses the HTML response from /rawlogs endpoint to extract rawlog items
func (f *RemoteFetcher) parseRawlogIndexHTML(html string) ([]receiver.RawLogItem, error) {
	var items []receiver.RawLogItem

	// Look for links to rawlog items in the format /rawlog/{timestamp}
	// This regex matches href="/rawlog/{timestamp}" where timestamp is a number
	linkPattern := regexp.MustCompile(`href="/rawlog/(\d+)"`)
	matches := linkPattern.FindAllStringSubmatch(html, -1)

	if len(matches) == 0 {
		logrus.Warn("No rawlog items found in HTML response")
		return items, nil
	}

	// Also look for timestamp text in the HTML to get formatted times
	// This is more flexible and can handle various HTML structures
	timestampPattern := regexp.MustCompile(`(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+UTC)`)
	timestampMatches := timestampPattern.FindAllString(html, -1)

	for i, match := range matches {
		timestampStr := match[1]

		// Parse timestamp from nanoseconds
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			logrus.Warnf("Failed to parse timestamp %s: %v", timestampStr, err)
			continue
		}

		ts := time.Unix(0, timestamp)

		// Use formatted timestamp from HTML if available, otherwise format ourselves
		var formattedTime string
		if i < len(timestampMatches) {
			formattedTime = timestampMatches[i]
		} else {
			formattedTime = receiver.FormatTimestamp(ts)
		}

		items = append(items, receiver.RawLogItem{
			ID:            timestampStr,
			Timestamp:     ts,
			FormattedTime: formattedTime,
		})
	}

	// Sort by timestamp, newest first (same as objectstorage implementation)
	for i := 0; i < len(items)-1; i++ {
		for j := i + 1; j < len(items); j++ {
			if items[i].Timestamp.Before(items[j].Timestamp) {
				items[i], items[j] = items[j], items[i]
			}
		}
	}

	return items, nil
}

// parseRawlogContentHTML parses the HTML response from /rawlog/{id} endpoint to extract raw log content
func (f *RemoteFetcher) parseRawlogContentHTML(html string) (string, error) {
	// Look for content within <pre> tags, which is commonly used for raw log display
	// Use (?s) flag to make . match newlines
	prePattern := regexp.MustCompile(`(?s)<pre[^>]*>(.*?)</pre>`)
	matches := prePattern.FindStringSubmatch(html)

	if len(matches) >= 2 {
		// Unescape HTML entities in the content
		content := matches[1]
		content = strings.ReplaceAll(content, "&lt;", "<")
		content = strings.ReplaceAll(content, "&gt;", ">")
		content = strings.ReplaceAll(content, "&amp;", "&")
		content = strings.ReplaceAll(content, "&#34;", "\"")
		content = strings.ReplaceAll(content, "&#39;", "'")

		return content, nil
	}

	// Fallback: look for content in a div with class "content" or similar
	contentPattern := regexp.MustCompile(`(?s)<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>`)
	matches = contentPattern.FindStringSubmatch(html)

	if len(matches) >= 2 {
		content := matches[1]
		// Remove HTML tags and unescape entities
		content = regexp.MustCompile(`<[^>]*>`).ReplaceAllString(content, "")
		content = strings.ReplaceAll(content, "&lt;", "<")
		content = strings.ReplaceAll(content, "&gt;", ">")
		content = strings.ReplaceAll(content, "&amp;", "&")
		content = strings.ReplaceAll(content, "&#34;", "\"")
		content = strings.ReplaceAll(content, "&#39;", "'")

		return content, nil
	}

	// Final fallback: extract everything between <body> tags and clean it up
	bodyPattern := regexp.MustCompile(`(?s)<body[^>]*>(.*?)</body>`)
	matches = bodyPattern.FindStringSubmatch(html)

	if len(matches) >= 2 {
		content := matches[1]
		// Remove script and style tags completely
		content = regexp.MustCompile(`(?s)<script[^>]*>.*?</script>`).ReplaceAllString(content, "")
		content = regexp.MustCompile(`(?s)<style[^>]*>.*?</style>`).ReplaceAllString(content, "")
		// Remove HTML tags
		content = regexp.MustCompile(`<[^>]*>`).ReplaceAllString(content, "")
		// Clean up whitespace
		content = regexp.MustCompile(`\s+`).ReplaceAllString(content, " ")
		content = strings.TrimSpace(content)

		// Only return if we have meaningful content
		if len(content) > 10 {
			return content, nil
		}
	}

	return "", fmt.Errorf("no rawlog content found in HTML response")
}

// cleanupContentCache removes old entries from the content cache to prevent memory leaks
// This should be called while holding the cache write lock
func (f *RemoteFetcher) cleanupContentCache() {
	// Simple cleanup: if we have more than 100 items, remove half of them
	// In a real implementation, you'd want to track access times and remove LRU items
	if len(f.cache.content) > 100 {
		count := 0
		for key := range f.cache.content {
			delete(f.cache.content, key)
			count++
			if count >= 50 {
				break
			}
		}
		logrus.Debug("Cleaned up content cache, removed 50 entries")
	}
}

// ClearCache clears all cached data
func (f *RemoteFetcher) ClearCache() {
	f.cache.mu.Lock()
	defer f.cache.mu.Unlock()

	f.cache.items = nil
	f.cache.itemsExpiry = time.Time{}
	f.cache.content = make(map[string][]byte)

	logrus.Debug("Cleared all cached data")
}

// SetCacheTTL allows configuring cache TTL values
func (f *RemoteFetcher) SetCacheTTL(itemsTTL, contentTTL time.Duration) {
	f.cache.mu.Lock()
	defer f.cache.mu.Unlock()

	f.cache.itemsTTL = itemsTTL
	f.cache.contentTTL = contentTTL

	logrus.Debugf("Updated cache TTL: items=%v, content=%v", itemsTTL, contentTTL)
}
