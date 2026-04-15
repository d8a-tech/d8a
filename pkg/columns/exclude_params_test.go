package columns

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStripExcludedParams(t *testing.T) {
	saveAndRestoreExcludedURLParams(t)

	tests := []struct {
		name           string
		inputURL       string
		excludedParams []string
		expectedURL    string
	}{
		{
			name:           "single custom param stripped",
			inputURL:       "https://example.org/?something=x&state=23r24r342&what=somewhat",
			excludedParams: []string{"state"},
			expectedURL:    "https://example.org/?something=x&what=somewhat",
		},
		{
			name:           "multiple custom params stripped",
			inputURL:       "https://example.org/?a=1&state=x&token=y&b=2",
			excludedParams: []string{"state", "token"},
			expectedURL:    "https://example.org/?a=1&b=2",
		},
		{
			name:           "custom param not present in url",
			inputURL:       "https://example.org/?a=1&b=2",
			excludedParams: []string{"state"},
			expectedURL:    "https://example.org/?a=1&b=2",
		},
		{
			name:           "empty url",
			inputURL:       "",
			excludedParams: []string{"state"},
			expectedURL:    "",
		},
		{
			name:           "url with no query string",
			inputURL:       "https://example.org/page",
			excludedParams: []string{"state"},
			expectedURL:    "https://example.org/page",
		},
		{
			name:           "custom and built in params stripped together",
			inputURL:       "https://example.org/?utm_source=g&state=x&foo=bar",
			excludedParams: []string{"utm_source", "state"},
			expectedURL:    "https://example.org/?foo=bar",
		},
		{
			name:           "all params stripped leaves clean url single",
			inputURL:       "https://example.org/?state=x",
			excludedParams: []string{"state"},
			expectedURL:    "https://example.org/",
		},
		{
			name:           "all params stripped leaves clean url multiple",
			inputURL:       "https://example.org/?state=x&token=y&sid=z",
			excludedParams: []string{"state", "token", "sid"},
			expectedURL:    "https://example.org/",
		},
		{
			name:           "param with empty value stripped",
			inputURL:       "https://example.org/?state=&foo=bar",
			excludedParams: []string{"state"},
			expectedURL:    "https://example.org/?foo=bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			resetExcludedURLParams()
			for _, param := range tt.excludedParams {
				RegisterURLParamForExclusion(param)
			}

			// when
			cleaned, original, err := StripExcludedParams(tt.inputURL)

			// then
			assert.NoError(t, err)
			assert.Equal(t, tt.inputURL, original)
			assert.Equal(t, tt.expectedURL, cleaned)
		})
	}
}

func saveAndRestoreExcludedURLParams(t *testing.T) {
	t.Helper()

	original := GetExcludedURLParams()
	t.Cleanup(func() {
		urlParamsBlacklistMu.Lock()
		defer urlParamsBlacklistMu.Unlock()

		urlParamsBlacklist = make(map[string]bool, len(original))
		for key, value := range original {
			urlParamsBlacklist[key] = value
		}
	})
}

func resetExcludedURLParams() {
	urlParamsBlacklistMu.Lock()
	defer urlParamsBlacklistMu.Unlock()

	urlParamsBlacklist = make(map[string]bool)
}
