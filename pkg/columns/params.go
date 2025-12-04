package columns

import (
	"net/url"
	"sync"

	"github.com/d8a-tech/d8a/pkg/schema"
)

const (
	// MetadataKeyOriginalPageLocation is the key used to store the original page location
	// (with tracking parameters) in event metadata.
	MetadataKeyOriginalPageLocation = "original_page_location"
)

var (
	// urlParamsBlacklist is a singleton registry of URL parameters that should be
	// excluded from page location URLs once they've been extracted into dedicated columns.
	urlParamsBlacklist   = make(map[string]bool)
	urlParamsBlacklistMu sync.RWMutex
)

// RegisterURLParamForExclusion adds a URL parameter to the blacklist registry.
// Parameters in this registry will be stripped from page location URLs.
func RegisterURLParamForExclusion(param string) {
	urlParamsBlacklistMu.Lock()
	defer urlParamsBlacklistMu.Unlock()
	urlParamsBlacklist[param] = true
}

// IsURLParamExcluded checks if a URL parameter is in the blacklist registry.
func IsURLParamExcluded(param string) bool {
	urlParamsBlacklistMu.RLock()
	defer urlParamsBlacklistMu.RUnlock()
	return urlParamsBlacklist[param]
}

// GetExcludedURLParams returns a copy of all excluded URL parameters.
func GetExcludedURLParams() map[string]bool {
	urlParamsBlacklistMu.RLock()
	defer urlParamsBlacklistMu.RUnlock()
	result := make(map[string]bool, len(urlParamsBlacklist))
	for k, v := range urlParamsBlacklist {
		result[k] = v
	}
	return result
}

// StripExcludedParams removes excluded URL parameters from a URL string.
// Returns the cleaned URL and the original URL.
func StripExcludedParams(urlStr string) (cleaned, original string, err error) {
	original = urlStr
	if urlStr == "" {
		return urlStr, original, nil
	}

	parsed, err := url.Parse(urlStr)
	if err != nil {
		return urlStr, original, err
	}

	query := parsed.Query()
	excludedParams := GetExcludedURLParams()
	removedAny := false

	for param := range excludedParams {
		if query.Has(param) {
			query.Del(param)
			removedAny = true
		}
	}

	if !removedAny {
		return urlStr, original, nil
	}

	parsed.RawQuery = query.Encode()
	cleaned = parsed.String()
	return cleaned, original, nil
}

// WriteOriginalPageLocation stores the original page location (with tracking parameters)
// in the event metadata.
func WriteOriginalPageLocation(event *schema.Event, originalURL string) {
	if event.Metadata == nil {
		event.Metadata = make(map[string]any)
	}
	event.Metadata[MetadataKeyOriginalPageLocation] = originalURL
}

// ReadOriginalPageLocation retrieves the original page location from event metadata.
// Returns empty string if not found.
func ReadOriginalPageLocation(event *schema.Event) string {
	if event.Metadata == nil {
		return ""
	}
	original, ok := event.Metadata[MetadataKeyOriginalPageLocation]
	if !ok {
		return ""
	}
	originalStr, ok := original.(string)
	if !ok {
		return ""
	}
	return originalStr
}
