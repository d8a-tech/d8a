package dbip

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingDownloader struct {
	calls atomic.Int32
	err   error
	path  string
}

func (d *countingDownloader) Download(context.Context, string, string, string) (string, error) {
	d.calls.Add(1)
	if d.err != nil {
		return "", d.err
	}
	return d.path, nil
}

func TestUnavailableLookupProvider(t *testing.T) {
	provider := NewUnavailableLookupProvider()

	_, err := provider.Lookup(netip.MustParseAddr("1.1.1.1"))

	assert.ErrorIs(t, err, ErrUnavailable)
}

func TestStaticLookupProvider(t *testing.T) {
	provider := NewStaticLookupProvider(&LookupResult{
		City:      "Wroclaw",
		Country:   "Poland",
		Continent: "Europe",
		Region:    "Lower Silesia",
	}, nil)

	res, err := provider.Lookup(netip.MustParseAddr("1.1.1.1"))

	require.NoError(t, err)
	assert.Equal(t, "Wroclaw", res.City)
	assert.Equal(t, "Poland", res.Country)
	assert.Equal(t, "Europe", res.Continent)
	assert.Equal(t, "Lower Silesia", res.Region)
}

func TestManagedLookupProvider_Disabled_DoesNotDownload(t *testing.T) {
	downloader := &countingDownloader{}
	provider := NewManagedLookupProvider(
		WithEnabled(false),
		WithDownloader(downloader),
		WithRefreshInterval(time.Millisecond),
		WithDestination(t.TempDir()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	provider.Run(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()

	assert.Equal(t, int32(0), downloader.calls.Load())
	require.NoError(t, provider.Close())
}

func TestManagedLookupProvider_RunInvokesDownloader(t *testing.T) {
	downloader := &countingDownloader{err: errors.New("downstream unavailable")}
	provider := NewManagedLookupProvider(
		WithEnabled(true),
		WithDownloader(downloader),
		WithRefreshInterval(time.Hour),
		WithDestination(t.TempDir()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider.Run(ctx)

	assert.Eventually(t, func() bool {
		return downloader.calls.Load() >= 1
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, provider.Close())
}

func TestManagedLookupProvider_RunStartsOnlyOnce(t *testing.T) {
	downloader := &countingDownloader{err: errors.New("downstream unavailable")}
	provider := NewManagedLookupProvider(
		WithEnabled(true),
		WithDownloader(downloader),
		WithRefreshInterval(time.Hour),
		WithDestination(t.TempDir()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider.Run(ctx)
	provider.Run(ctx)

	assert.Eventually(t, func() bool {
		return downloader.calls.Load() >= 1
	}, time.Second, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(1), downloader.calls.Load())
	require.NoError(t, provider.Close())
}

func TestManagedLookupProvider_MissingDirectory_IsSilentAndUnavailable(t *testing.T) {
	downloader := &countingDownloader{err: errors.New("should not run")}
	missingDir := filepath.Join(t.TempDir(), "missing")

	provider := NewManagedLookupProvider(
		WithEnabled(true),
		WithDownloader(downloader),
		WithRefreshInterval(IntervalNever),
		WithDestination(missingDir),
	)

	ctx, cancel := context.WithCancel(context.Background())
	provider.Run(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()

	assert.NoFileExists(t, filepath.Join(missingDir, "anything.mmdb"))
	assert.Equal(t, int32(0), downloader.calls.Load())

	_, err := provider.Lookup(netip.MustParseAddr("1.1.1.1"))
	assert.ErrorIs(t, err, ErrUnavailable)
	require.NoError(t, provider.Close())
}

func TestManagedLookupProvider_DefaultsToProjectDirectory(t *testing.T) {
	provider := NewManagedLookupProvider()
	managed, ok := provider.(*managedProvider)
	require.True(t, ok)

	assert.Equal(t, "./dbip", managed.destinationDirectory)
	require.NoError(t, managed.Close())
}

func TestManagedLookupProvider_DestinationOverride(t *testing.T) {
	customDir := filepath.Join(t.TempDir(), "dbip-custom")
	provider := NewManagedLookupProvider(WithDestination(customDir))
	managed, ok := provider.(*managedProvider)
	require.True(t, ok)

	assert.Equal(t, customDir, managed.destinationDirectory)
	require.NoError(t, managed.Close())
}

func TestManagedLookupProvider_RefreshFromLocal_WhenDirectoryExistsAndEmpty(t *testing.T) {
	localDir := t.TempDir()
	provider := NewManagedLookupProvider(
		WithDestination(localDir),
		WithRefreshInterval(IntervalNever),
	)
	managed, ok := provider.(*managedProvider)
	require.True(t, ok)

	managed.refreshFromLocal()

	_, statErr := os.Stat(localDir)
	require.NoError(t, statErr)
	_, lookupErr := managed.Lookup(netip.MustParseAddr("1.1.1.1"))
	assert.ErrorIs(t, lookupErr, ErrUnavailable)
	require.NoError(t, managed.Close())
}
