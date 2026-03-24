package dbip

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/sirupsen/logrus"
)

const (
	defaultDestinationDir = "/tmp/dbip"
	defaultRefreshEvery   = 6 * time.Hour
	localScanEvery        = 30 * time.Second
	datasetArtifact       = "dbip-city-lite"
	datasetTag            = "latest"
)

// IntervalNever disables remote DBIP refreshes.
const IntervalNever = time.Duration(0)

// ErrUnavailable means DBIP geolocation is not currently available.
var ErrUnavailable = errors.New("dbip lookup unavailable")

// Runner starts background DBIP refresh work.
type Runner interface {
	Run(ctx context.Context)
}

// ManagedLookupProvider combines lookup and refresh lifecycle.
type ManagedLookupProvider interface {
	LookupProvider
	Runner
	Close() error
}

type managerOption func(*managedProvider)

type managedProvider struct {
	enabled              bool
	downloader           Downloader
	destinationDirectory string
	refreshEvery         time.Duration
	downloadTimeout      time.Duration

	runOnce sync.Once

	mu     sync.RWMutex
	reader *maxminddb.Reader
	path   string
}

type unavailableLookupProvider struct{}

type staticLookupProvider struct {
	result *LookupResult
	err    error
}

// NewUnavailableLookupProvider always reports unavailable lookups.
func NewUnavailableLookupProvider() LookupProvider {
	return &unavailableLookupProvider{}
}

// NewStaticLookupProvider returns a provider with deterministic behavior for tests.
func NewStaticLookupProvider(result *LookupResult, err error) LookupProvider {
	return &staticLookupProvider{result: result, err: err}
}

// WithEnabled configures whether DBIP manager is active.
func WithEnabled(enabled bool) managerOption {
	return func(m *managedProvider) {
		m.enabled = enabled
	}
}

// WithDownloader configures DBIP downloader.
func WithDownloader(downloader Downloader) managerOption {
	return func(m *managedProvider) {
		if downloader != nil {
			m.downloader = downloader
		}
	}
}

// WithDestination configures DBIP files destination directory.
func WithDestination(destinationDirectory string) managerOption {
	return func(m *managedProvider) {
		if destinationDirectory != "" {
			m.destinationDirectory = destinationDirectory
		}
	}
}

// WithRefreshInterval configures remote DBIP refresh interval.
func WithRefreshInterval(interval time.Duration) managerOption {
	return func(m *managedProvider) {
		m.refreshEvery = interval
	}
}

// WithDownloadTimeout configures DBIP remote download timeout.
func WithDownloadTimeout(timeout time.Duration) managerOption {
	return func(m *managedProvider) {
		if timeout > 0 {
			m.downloadTimeout = timeout
		}
	}
}

// NewManagedLookupProvider creates a DBIP lookup provider with optional background refresh.
func NewManagedLookupProvider(options ...managerOption) ManagedLookupProvider {
	provider := &managedProvider{
		enabled: true,
		downloader: NewExtensionBasedOCIDownloader(
			OCIRegistryCreds{
				Repo:       "ghcr.io/d8a-tech",
				IgnoreCert: false,
			},
			".mmdb",
		),
		destinationDirectory: defaultDestinationDir,
		refreshEvery:         defaultRefreshEvery,
		downloadTimeout:      60 * time.Second,
	}

	for _, option := range options {
		option(provider)
	}

	return provider
}

// Lookup implements LookupProvider.
func (m *managedProvider) Lookup(ip netip.Addr) (*LookupResult, error) {
	m.mu.RLock()
	reader := m.reader
	m.mu.RUnlock()

	if reader == nil {
		return nil, ErrUnavailable
	}

	var record result
	if err := reader.Lookup(ip).Decode(&record); err != nil {
		return nil, err
	}

	return recordToLookupResult(&record), nil
}

// Run implements Runner.
func (m *managedProvider) Run(ctx context.Context) {
	m.runOnce.Do(func() {
		if !m.enabled {
			return
		}

		go m.refreshLoop(ctx)
	})
}

// Close releases currently active reader resources.
func (m *managedProvider) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.reader == nil {
		return nil
	}

	err := m.reader.Close()
	m.reader = nil
	m.path = ""
	if err != nil {
		return fmt.Errorf("close dbip reader: %w", err)
	}
	return nil
}

func (m *managedProvider) refreshLoop(ctx context.Context) {
	m.refreshFromLocal()
	if m.refreshEvery != IntervalNever {
		m.refreshFromRemote(ctx)
	}

	localTicker := time.NewTicker(localScanEvery)
	defer localTicker.Stop()

	var remoteTicker *time.Ticker
	if m.refreshEvery != IntervalNever {
		remoteTicker = time.NewTicker(m.refreshEvery)
		defer remoteTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-localTicker.C:
			m.refreshFromLocal()
		case <-func() <-chan time.Time {
			if remoteTicker == nil {
				return nil
			}
			return remoteTicker.C
		}():
			m.refreshFromRemote(ctx)
		}
	}
}

func (m *managedProvider) refreshFromRemote(ctx context.Context) {
	if m.downloader == nil {
		return
	}

	downloadCtx, cancel := context.WithTimeout(ctx, m.downloadTimeout)
	defer cancel()

	path, err := m.downloader.Download(downloadCtx, datasetArtifact, datasetTag, m.destinationDirectory)
	if err != nil {
		logrus.WithError(err).Warn("dbip: remote refresh failed")
		return
	}

	m.activatePath(path)
}

func (m *managedProvider) refreshFromLocal() {
	path, err := selectBestMMDBFile(m.destinationDirectory, ".mmdb")
	if err != nil {
		logrus.WithError(err).Warn("dbip: failed to inspect local MMDB files")
		return
	}
	if path == "" {
		return
	}

	m.activatePath(path)
}

func (m *managedProvider) activatePath(path string) {
	m.mu.RLock()
	currentPath := m.path
	m.mu.RUnlock()

	if path == currentPath {
		return
	}

	reader, err := maxminddb.Open(path)
	if err != nil {
		logrus.WithError(err).WithField("path", path).Warn("dbip: failed to open MMDB file")
		return
	}

	m.mu.Lock()
	oldReader := m.reader
	m.reader = reader
	m.path = path
	m.mu.Unlock()

	if oldReader != nil {
		if err := oldReader.Close(); err != nil {
			logrus.WithError(err).Warn("dbip: failed to close old MMDB reader")
		}
	}
}

func recordToLookupResult(record *result) *LookupResult {
	region := ""
	if len(record.Subdivisions) > 0 {
		region = record.Subdivisions[0].Names.English
	}

	return &LookupResult{
		City:      record.City.Names.English,
		Country:   record.Country.Names.English,
		Continent: record.Continent.Names.English,
		Region:    region,
	}
}

func (u *unavailableLookupProvider) Lookup(netip.Addr) (*LookupResult, error) {
	return nil, ErrUnavailable
}

func (s *staticLookupProvider) Lookup(netip.Addr) (*LookupResult, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.result == nil {
		return nil, ErrUnavailable
	}
	return &LookupResult{
		City:      s.result.City,
		Country:   s.result.Country,
		Continent: s.result.Continent,
		Region:    s.result.Region,
	}, nil
}
