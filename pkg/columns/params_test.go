package columns

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStripExcludedParams(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		urlStr         string
		excludedParams []string
		expectedURL    string
		expectedErr    string
	}{
		{
			name:           "strips explicitly excluded params",
			urlStr:         "https://example.com/path?utm_source=google&foo=bar&gclid=123",
			excludedParams: []string{"utm_source", "gclid"},
			expectedURL:    "https://example.com/path?foo=bar",
		},
		{
			name:           "empty exclusion list keeps url unchanged",
			urlStr:         "https://example.com/path?utm_source=google&foo=bar",
			excludedParams: []string{},
			expectedURL:    "https://example.com/path?utm_source=google&foo=bar",
		},
		{
			name:           "non matching params keep url unchanged",
			urlStr:         "https://example.com/path?foo=bar",
			excludedParams: []string{"utm_source", "gclid"},
			expectedURL:    "https://example.com/path?foo=bar",
		},
		{
			name:           "invalid url returns parse error",
			urlStr:         "http://[::1",
			excludedParams: []string{"utm_source"},
			expectedURL:    "http://[::1",
			expectedErr:    "missing ']' in host",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// given

			// when
			cleaned, original, err := StripExcludedParams(testCase.urlStr, testCase.excludedParams)

			// then
			if testCase.expectedErr != "" {
				assert.Error(t, err)
				assert.ErrorContains(t, err, testCase.expectedErr)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, testCase.urlStr, original)
			assert.Equal(t, testCase.expectedURL, cleaned)
		})
	}
}
