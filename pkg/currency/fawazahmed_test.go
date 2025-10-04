package currency

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { // implements http.RoundTripper
	return f(r)
}

func newMockClient(fn roundTripFunc) *http.Client {
	return &http.Client{Transport: fn}
}

func httpJSON(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}

func TestFawazAhmedConverter_Convert_ArrayDriven(t *testing.T) {
	// given
	base := "usd"
	jsURL := fmt.Sprintf("%s/%s.json", jsDelivrBase, base)
	cfURL := fmt.Sprintf("%s/%s.json", cloudflareBase, base)

	tests := []struct {
		name      string
		jsStatus  int
		jsBody    string
		jsErr     error
		cfStatus  int
		cfBody    string
		cfErr     error
		from      string
		to        string
		amount    float64
		expectVal float64
		expectErr bool
	}{
		{
			name:      "same currency bypasses network and returns amount",
			from:      "USD",
			to:        "USD",
			amount:    12.34,
			expectVal: 12.34,
		},
		{
			name:      "on-demand fetch from primary CDN succeeds",
			jsStatus:  200,
			jsBody:    `{"usd":{"eur":0.9,"gbp":0.8}}`,
			from:      "USD",
			to:        "EUR",
			amount:    10,
			expectVal: 9,
		},
		{
			name:      "primary fails, fallback succeeds",
			jsStatus:  500,
			jsBody:    "",
			cfStatus:  200,
			cfBody:    `{"usd":{"jpy":150}}`,
			from:      "USD",
			to:        "JPY",
			amount:    2,
			expectVal: 300,
		},
		{
			name:      "rate not found in payload",
			jsStatus:  200,
			jsBody:    `{"usd":{"gbp":0.8}}`,
			from:      "USD",
			to:        "EUR",
			amount:    10,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			convI, err := NewFawazAhmedConverter([]string{})
			assert.NoError(t, err)
			conv, ok := convI.(*FawazAhmedConverter)
			if !ok {
				t.Fatalf("unexpected converter type: %T", convI)
			}
			conv.httpClient = newMockClient(func(r *http.Request) (*http.Response, error) {
				switch r.URL.String() {
				case jsURL:
					if tt.jsErr != nil {
						return nil, tt.jsErr
					}
					if tt.jsStatus != 0 {
						return httpJSON(tt.jsStatus, tt.jsBody), nil
					}
				case cfURL:
					if tt.cfErr != nil {
						return nil, tt.cfErr
					}
					if tt.cfStatus != 0 {
						return httpJSON(tt.cfStatus, tt.cfBody), nil
					}
				}
				return httpJSON(404, ""), nil
			})

			val, cErr := conv.Convert(tt.from, tt.to, tt.amount)

			// responses are created and consumed per request; nothing to close here

			// then
			if tt.expectErr {
				assert.Error(t, cErr)
				return
			}
			assert.NoError(t, cErr)
			assert.Equal(t, tt.expectVal, val)
		})
	}
}

func TestFawazAhmedConverter_FetchRates_MalformedPayload(t *testing.T) {
	// given
	base := "usd"
	jsURL := fmt.Sprintf("%s/%s.json", jsDelivrBase, base)
	convI, err := NewFawazAhmedConverter([]string{})
	assert.NoError(t, err)
	conv, ok := convI.(*FawazAhmedConverter)
	assert.True(t, ok)
	conv.httpClient = newMockClient(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() == jsURL {
			return httpJSON(200, `{"usd":{"eur":"bad"}}`), nil
		}
		return httpJSON(404, ""), nil
	})

	// when
	_, ferr := conv.fetchRates(base)

	// then
	assert.Error(t, ferr)
}
