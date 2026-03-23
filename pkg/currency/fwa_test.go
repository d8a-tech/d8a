package currency

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type downloaderFunc func(context.Context) (*Snapshot, error)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
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

func (f downloaderFunc) Download(ctx context.Context) (*Snapshot, error) {
	return f(ctx)
}

func TestFWAConverter_LoadsLatestSnapshotFromStore(t *testing.T) {
	// given
	dir := t.TempDir()
	store := NewFileStore(dir)
	require.NoError(t, store.Append(&Snapshot{
		CreatedAt: time.Date(2026, 3, 20, 9, 0, 0, 0, time.UTC),
		Rates: map[string]map[string]float64{
			"usd": {"eur": 0.9},
		},
	}))
	require.NoError(t, store.Append(&Snapshot{
		CreatedAt: time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
		Rates: map[string]map[string]float64{
			"usd": {"eur": 0.95},
		},
	}))

	// when
	converter, err := NewFWAConverter(nil, WithStore(store), WithInterval(IntervalNever))

	// then
	require.NoError(t, err)
	assert.True(t, converter.HasSnapshot())
	value, err := converter.Convert("USD", "EUR", 10)
	require.NoError(t, err)
	assert.Equal(t, 9.5, value)
}

func TestFWAConverter_ConvertReturnsUnavailableWithoutSnapshot(t *testing.T) {
	// given
	converter, err := NewFWAConverter(nil, WithStore(NewFileStore(t.TempDir())), WithInterval(IntervalNever))
	require.NoError(t, err)

	// when
	_, err = converter.Convert("USD", "EUR", 10)

	// then
	assert.ErrorIs(t, err, ErrUnavailable)
	assert.False(t, converter.HasSnapshot())
}

func TestFWAConverter_RunRefreshesAndPersistsSnapshot(t *testing.T) {
	// given
	dir := t.TempDir()
	refreshDone := make(chan struct{}, 1)
	converter, err := NewFWAConverter(
		nil,
		WithStore(NewFileStore(dir)),
		WithInterval(10*time.Millisecond),
		WithDownloader(downloaderFunc(func(context.Context) (*Snapshot, error) {
			select {
			case refreshDone <- struct{}{}:
			default:
			}
			return &Snapshot{
				CreatedAt: time.Date(2026, 3, 22, 8, 0, 0, 0, time.UTC),
				Rates: map[string]map[string]float64{
					"usd": {"eur": 0.91},
				},
			}, nil
		})),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// when
	converter.Run(ctx)

	select {
	case <-refreshDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected refresh to run")
	}

	// then
	assert.Eventually(t, func() bool {
		value, convErr := converter.Convert("USD", "EUR", 10)
		return convErr == nil && value == 9.1
	}, time.Second, 10*time.Millisecond)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.NotEmpty(t, entries)
}

func TestFWAConverter_RunRefreshesImmediatelyWithoutSnapshot(t *testing.T) {
	// given
	refreshDone := make(chan struct{}, 1)
	converter, err := NewFWAConverter(
		nil,
		WithStore(NewFileStore(t.TempDir())),
		WithInterval(time.Hour),
		WithDownloader(downloaderFunc(func(context.Context) (*Snapshot, error) {
			select {
			case refreshDone <- struct{}{}:
			default:
			}
			return &Snapshot{
				CreatedAt: time.Date(2026, 3, 23, 8, 0, 0, 0, time.UTC),
				Rates: map[string]map[string]float64{
					"usd": {"eur": 0.92},
				},
			}, nil
		})),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// when
	converter.Run(ctx)

	// then
	select {
	case <-refreshDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected immediate refresh on startup")
	}

	assert.Eventually(t, func() bool {
		value, convErr := converter.Convert("USD", "EUR", 10)
		return convErr == nil && assert.InDelta(t, 9.2, value, 0.0001)
	}, time.Second, 10*time.Millisecond)
}

func TestFWAConverter_RefreshFailureKeepsExistingSnapshot(t *testing.T) {
	// given
	dir := t.TempDir()
	store := NewFileStore(dir)
	require.NoError(t, store.Append(&Snapshot{
		CreatedAt: time.Date(2026, 3, 20, 9, 0, 0, 0, time.UTC),
		Rates: map[string]map[string]float64{
			"usd": {"eur": 0.8},
		},
	}))

	converter, err := NewFWAConverter(
		nil,
		WithStore(store),
		WithInterval(10*time.Millisecond),
		WithDownloader(downloaderFunc(func(context.Context) (*Snapshot, error) {
			return nil, errors.New("boom")
		})),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// when
	converter.Run(ctx)

	// then
	assert.Eventually(t, func() bool {
		value, convErr := converter.Convert("USD", "EUR", 10)
		return convErr == nil && value == 8
	}, 300*time.Millisecond, 10*time.Millisecond)
}

func TestFileStoreLatestSkipsInvalidNewestSnapshot(t *testing.T) {
	// given
	dir := t.TempDir()
	store := NewFileStore(dir)
	require.NoError(t, store.Append(&Snapshot{
		CreatedAt: time.Date(2026, 3, 20, 9, 0, 0, 0, time.UTC),
		Rates: map[string]map[string]float64{
			"usd": {"eur": 0.8},
		},
	}))
	invalidPath := filepath.Join(dir, "rates-99999999T999999.000000000Z.json")
	require.NoError(t, os.WriteFile(invalidPath, []byte(`{"created_at":"2026-03-22T09:00:00Z","rates":{}}`), 0o600))

	// when
	snapshot, err := store.Latest()

	// then
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.Equal(t, time.Date(2026, 3, 20, 9, 0, 0, 0, time.UTC), snapshot.CreatedAt)
}

func TestAPIDownloader_Download_ArrayDriven(t *testing.T) {
	// given
	jsURL := func(base string) string {
		return jsDelivrBase + "/" + base + ".json"
	}
	cfURL := func(base string) string {
		return cloudflareBase + "/" + base + ".json"
	}

	tests := []struct {
		name      string
		bases     []string
		responses map[string]struct {
			status int
			body   string
		}
		errors      map[string]error
		expectRates map[string]map[string]float64
		expectErr   bool
	}{
		{
			name:  "downloads all requested bases",
			bases: []string{"USD", "EUR"},
			responses: map[string]struct {
				status int
				body   string
			}{
				jsURL("usd"): {status: 200, body: `{"usd":{"eur":0.9}}`},
				jsURL("eur"): {status: 200, body: `{"eur":{"usd":1.1}}`},
			},
			expectRates: map[string]map[string]float64{
				"usd": {"eur": 0.9},
				"eur": {"usd": 1.1},
			},
		},
		{
			name:  "falls back to secondary endpoint",
			bases: []string{"USD"},
			responses: map[string]struct {
				status int
				body   string
			}{
				jsURL("usd"): {status: 500, body: ""},
				cfURL("usd"): {status: 200, body: `{"usd":{"gbp":0.8}}`},
			},
			expectRates: map[string]map[string]float64{
				"usd": {"gbp": 0.8},
			},
		},
		{
			name:  "returns error on malformed payload",
			bases: []string{"USD"},
			responses: map[string]struct {
				status int
				body   string
			}{
				jsURL("usd"): {status: 200, body: `{"usd":{"eur":"bad"}}`},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			downloader := &apiDownloader{
				httpClient: newMockClient(func(r *http.Request) (*http.Response, error) {
					if err := tt.errors[r.URL.String()]; err != nil {
						return nil, err
					}
					if response, ok := tt.responses[r.URL.String()]; ok {
						return httpJSON(response.status, response.body), nil
					}
					return httpJSON(404, ""), nil
				}),
				bases: tt.bases,
			}

			snapshot, err := downloader.Download(context.Background())

			// then
			if tt.expectErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectRates, snapshot.Rates)
		})
	}
}
