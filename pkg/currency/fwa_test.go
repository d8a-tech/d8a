package currency

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type downloaderFunc func(context.Context) (*Snapshot, error)

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
