// nolint // this is debugger code, not for prod and 100% vibe-coded
package debugger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// RequestMetadata contains metadata about the HTTP request
type RequestMetadata struct {
	Timestamp string `json:"timestamp"`
	Method    string `json:"method"`
	Path      string `json:"path"`
}

// ParsedRequest represents a parsed HTTP request in structured format
type ParsedRequest struct {
	Metadata    RequestMetadata   `json:"metadata"`
	QueryParams map[string]string `json:"query_params"`
	Headers     map[string]string `json:"headers"`
}

// RequestParser handles parsing of raw HTTP requests from rawlog content
type RequestParser struct{}

// NewRequestParser creates a new RequestParser instance
func NewRequestParser() *RequestParser {
	return &RequestParser{}
}

// ParseRawlogContent parses raw HTTP request(s) from rawlog content and converts to structured JSON
func (p *RequestParser) ParseRawlogContent(content []byte) ([]ParsedRequest, error) {
	contentStr := string(content)

	// Split requests using a more sophisticated approach
	requestBlocks := p.splitHTTPRequests(contentStr)

	var parsedRequests []ParsedRequest

	for i, block := range requestBlocks {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}

		parsed, err := p.parseHTTPRequest(block)
		if err != nil {
			logrus.Warnf("Failed to parse request block %d: %v", i, err)
			continue
		}

		parsedRequests = append(parsedRequests, parsed)
	}

	if len(parsedRequests) == 0 {
		return nil, fmt.Errorf("no valid HTTP requests found in rawlog content")
	}

	return parsedRequests, nil
}

// splitHTTPRequests splits rawlog content into individual HTTP request blocks
func (p *RequestParser) splitHTTPRequests(content string) []string {
	var requests []string

	// HTTP request line pattern: METHOD /path HTTP/version
	httpRequestPattern := regexp.MustCompile(`(?m)^(GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH|TRACE)\s+\S+\s+HTTP/\d\.\d`)

	// Find all matches with their positions
	matches := httpRequestPattern.FindAllStringIndex(content, -1)

	if len(matches) == 0 {
		// No HTTP request lines found, return original content as single block
		return []string{content}
	}

	// Split content based on request line positions
	for i, match := range matches {
		start := match[0]

		var end int
		if i+1 < len(matches) {
			// Next request starts here, so current request ends just before
			end = matches[i+1][0]
		} else {
			// Last request, goes to end of content
			end = len(content)
		}

		requestBlock := strings.TrimSpace(content[start:end])
		if requestBlock != "" {
			requests = append(requests, requestBlock)
		}
	}

	return requests
}

// parseHTTPRequest parses a single HTTP request block into structured format
func (p *RequestParser) parseHTTPRequest(requestBlock string) (ParsedRequest, error) {
	var parsed ParsedRequest

	scanner := bufio.NewScanner(strings.NewReader(requestBlock))

	// Parse request line (first line)
	if !scanner.Scan() {
		return parsed, fmt.Errorf("empty request block")
	}

	requestLine := scanner.Text()
	method, path, err := p.parseRequestLine(requestLine)
	if err != nil {
		return parsed, fmt.Errorf("failed to parse request line: %w", err)
	}

	// Extract query parameters from path
	queryParams, cleanPath := p.extractQueryParams(path)

	// Set metadata
	parsed.Metadata = RequestMetadata{
		Timestamp: time.Now().UTC().Format(time.RFC3339), // Default timestamp, will be overridden if available
		Method:    method,
		Path:      cleanPath,
	}
	parsed.QueryParams = queryParams
	parsed.Headers = make(map[string]string)

	// Parse headers
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)

		// Empty line indicates end of headers
		if line == "" {
			break
		}

		// Parse header
		if colonIndex := strings.Index(line, ":"); colonIndex > 0 {
			key := strings.TrimSpace(line[:colonIndex])
			value := strings.TrimSpace(line[colonIndex+1:])

			// Normalize header key to lowercase for consistent comparison
			key = strings.ToLower(key)
			parsed.Headers[key] = value
		}
	}

	if err := scanner.Err(); err != nil {
		return parsed, fmt.Errorf("error scanning request: %w", err)
	}

	return parsed, nil
}

// parseRequestLine parses the HTTP request line (e.g., "POST /g/collect?v=2&tid=G-123 HTTP/1.1")
func (p *RequestParser) parseRequestLine(line string) (method, path string, err error) {
	// HTTP request line format: METHOD PATH HTTP/VERSION
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid request line format: %s", line)
	}

	method = strings.ToUpper(parts[0])
	path = parts[1]

	return method, path, nil
}

// extractQueryParams extracts query parameters from a URL path and returns them as a map
// along with the clean path (without query string)
func (p *RequestParser) extractQueryParams(path string) (queryParams map[string]string, cleanPath string) {
	queryParams = make(map[string]string)

	// Split path and query string
	parts := strings.SplitN(path, "?", 2)
	cleanPath = parts[0]

	if len(parts) < 2 {
		return queryParams, cleanPath
	}

	queryString := parts[1]

	// Parse query parameters
	params, err := url.ParseQuery(queryString)
	if err != nil {
		logrus.Warnf("Failed to parse query string '%s': %v", queryString, err)
		return queryParams, cleanPath
	}

	// Convert to simple string map (take first value for each key)
	for key, values := range params {
		if len(values) > 0 {
			queryParams[key] = values[0]
		}
	}

	return queryParams, cleanPath
}

// ConvertToJSON converts parsed requests to JSON format
func (p *RequestParser) ConvertToJSON(requests []ParsedRequest) ([]byte, error) {
	if len(requests) == 1 {
		// For single request, return the request directly
		return json.MarshalIndent(requests[0], "", "  ")
	}

	// For multiple requests, return as array
	return json.MarshalIndent(requests, "", "  ")
}

// ParseAndConvert is a convenience method that parses rawlog content and converts to JSON
func (p *RequestParser) ParseAndConvert(content []byte) ([]byte, error) {
	requests, err := p.ParseRawlogContent(content)
	if err != nil {
		return nil, err
	}

	return p.ConvertToJSON(requests)
}

// SetTimestamp updates the timestamp in parsed request metadata
func (p *RequestParser) SetTimestamp(parsed *ParsedRequest, timestamp time.Time) {
	parsed.Metadata.Timestamp = timestamp.UTC().Format(time.RFC3339)
}

// NormalizeHeaders applies case normalization to headers (already done in parseHTTPRequest)
func (p *RequestParser) NormalizeHeaders(headers map[string]string) map[string]string {
	normalized := make(map[string]string)
	for key, value := range headers {
		normalized[strings.ToLower(key)] = value
	}
	return normalized
}

// ValidateRequest performs basic validation on a parsed request
func (p *RequestParser) ValidateRequest(parsed ParsedRequest) error {
	if parsed.Metadata.Method == "" {
		return fmt.Errorf("missing HTTP method")
	}

	if parsed.Metadata.Path == "" {
		return fmt.Errorf("missing request path")
	}

	// Check for common required headers based on method
	if parsed.Metadata.Method == "POST" {
		if _, hasContentType := parsed.Headers["content-type"]; !hasContentType {
			logrus.Warn("POST request missing content-type header")
		}
	}

	return nil
}

// ExtractTimestampFromHeaders attempts to extract timestamp from request headers or query params
func (p *RequestParser) ExtractTimestampFromHeaders(parsed *ParsedRequest) {
	// Look for timestamp in common places
	timestampSources := []string{
		parsed.QueryParams["_t"],      // Common timestamp parameter
		parsed.QueryParams["t"],       // Alternative timestamp parameter
		parsed.Headers["x-timestamp"], // Custom timestamp header
		parsed.Headers["date"],        // Standard date header
	}

	for _, tsStr := range timestampSources {
		if tsStr == "" {
			continue
		}

		// Try parsing as Unix timestamp (milliseconds)
		if matched, _ := regexp.MatchString(`^\d{13}$`, tsStr); matched {
			if ts, err := time.Parse("", tsStr); err == nil {
				p.SetTimestamp(parsed, ts)
				return
			}
		}

		// Try parsing as Unix timestamp (seconds)
		if matched, _ := regexp.MatchString(`^\d{10}$`, tsStr); matched {
			if ts, err := time.Parse("", tsStr); err == nil {
				p.SetTimestamp(parsed, ts)
				return
			}
		}

		// Try parsing as RFC3339
		if ts, err := time.Parse(time.RFC3339, tsStr); err == nil {
			p.SetTimestamp(parsed, ts)
			return
		}

		// Try parsing as HTTP date format
		if ts, err := time.Parse(time.RFC1123, tsStr); err == nil {
			p.SetTimestamp(parsed, ts)
			return
		}
	}
}
