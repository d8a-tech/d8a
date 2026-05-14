package columns

import (
	"net/url"

	"github.com/d8a-tech/d8a/pkg/schema"
)

const (
	// MetadataKeyOriginalPageLocation is the key used to store the original page location
	// (with tracking parameters) in event metadata.
	MetadataKeyOriginalPageLocation = "original_page_location"

	MetadataKeySessionSourceMediumTerm = "session_source_medium_term"
)

// StripExcludedParams removes excluded URL parameters from a URL string.
// Returns the cleaned URL and the original URL.
func StripExcludedParams(urlStr string, excludedParams []string) (cleaned, original string, err error) {
	original = urlStr
	if urlStr == "" {
		return urlStr, original, nil
	}
	if len(excludedParams) == 0 {
		return urlStr, original, nil
	}

	parsed, err := url.Parse(urlStr)
	if err != nil {
		return urlStr, original, err
	}

	query := parsed.Query()
	removedAny := false

	for _, param := range excludedParams {
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
