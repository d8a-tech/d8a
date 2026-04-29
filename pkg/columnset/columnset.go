// Package columnset provides a default columnset for the tracker API.
package columnset

import (
	"time"

	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/columns/sessioncolumns"
	"github.com/d8a-tech/d8a/pkg/dbip"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocolschema"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// columnSetConfig holds the configuration for column set initialization.
type columnSetConfig struct {
	geoProvider           dbip.LookupProvider
	deviceColumns         []schema.EventColumn
	customColumnsRegistry []schema.ColumnsRegistry
}

// ColumnSetOption is a function that modifies columnSetConfig.
type ColumnSetOption func(*columnSetConfig)

// newDefaultColumnSetConfig returns a columnSetConfig initialized with stub implementations.
func newDefaultColumnSetConfig() *columnSetConfig {
	return &columnSetConfig{
		geoProvider: dbip.NewUnavailableLookupProvider(),
		deviceColumns: []schema.EventColumn{
			eventcolumns.DeviceCategoryStubColumn,
			eventcolumns.DeviceMobileBrandNameStubColumn,
			eventcolumns.DeviceMobileModelNameStubColumn,
			eventcolumns.DeviceOperatingSystemStubColumn,
			eventcolumns.DeviceOperatingSystemVersionStubColumn,
			eventcolumns.DeviceWebBrowserStubColumn,
			eventcolumns.DeviceWebBrowserVersionStubColumn,
		},
	}
}

// WithGeoProvider returns a ColumnSetOption that sets the DBIP lookup provider.
func WithGeoProvider(provider dbip.LookupProvider) ColumnSetOption {
	return func(cfg *columnSetConfig) {
		if provider == nil {
			panic("geo provider cannot be nil")
		}
		cfg.geoProvider = provider
	}
}

// WithDeviceDetectionColumns returns a ColumnSetOption that sets the device detection columns.
func WithDeviceDetectionColumns(cols []schema.EventColumn) ColumnSetOption {
	return func(cfg *columnSetConfig) {
		cfg.deviceColumns = cols
	}
}

// WithCustomColumnsRegistry returns a ColumnSetOption that sets a custom columns registry.
func WithCustomColumnsRegistry(reg schema.ColumnsRegistry) ColumnSetOption {
	return func(cfg *columnSetConfig) {
		if reg == nil {
			return
		}
		cfg.customColumnsRegistry = append(cfg.customColumnsRegistry, reg)
	}
}

// DefaultColumnRegistry returns a default column registry for the tracker API.
func DefaultColumnRegistry(
	theProtocol protocol.Protocol,
	psr properties.SettingsRegistry,
	opts ...ColumnSetOption,
) schema.ColumnsRegistry {
	cfg := newDefaultColumnSetConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	geoColumns := dbip.GeoColumns(
		cfg.geoProvider,
		dbip.CacheConfig{
			MaxEntries: 1024,
			TTL:        30 * time.Second,
		},
	)

	// Merge geo and device columns into a single slice for the static registry
	injectedColumns := make([]schema.EventColumn, 0, len(geoColumns)+len(cfg.deviceColumns))
	injectedColumns = append(injectedColumns, geoColumns...)
	injectedColumns = append(injectedColumns, cfg.deviceColumns...)

	registries := []schema.ColumnsRegistry{
		schema.NewStaticColumnsRegistry(
			map[string]schema.Columns{},
			schema.NewColumns(sessionColumns(), eventColumns(psr), sessionScopedEventColumns()),
		),
		protocolschema.NewFromProtocolColumnsRegistry(protocol.NewStaticRegistry(
			map[string]protocol.Protocol{},
			theProtocol,
		)),
		schema.NewStaticColumnsRegistry(
			map[string]schema.Columns{},
			schema.NewColumns([]schema.SessionColumn{}, injectedColumns, []schema.SessionScopedEventColumn{}),
		),
	}

	if len(cfg.customColumnsRegistry) > 0 {
		registries = append(registries, cfg.customColumnsRegistry...)
	}

	return schema.NewColumnsMerger(registries)
}

func eventColumns(psr properties.SettingsRegistry) []schema.EventColumn {
	return []schema.EventColumn{
		eventcolumns.EventIDColumn,
		eventcolumns.EventNameColumn,
		eventcolumns.IPAddressColumn,
		eventcolumns.ClientIDColumn,
		eventcolumns.UserIDColumn,
		eventcolumns.PropertyIDColumn,
		eventcolumns.PropertyNameColumn(psr),
		eventcolumns.UtmMarketingTacticColumn,
		eventcolumns.UtmSourcePlatformColumn,
		eventcolumns.UtmTermColumn,
		eventcolumns.UtmContentColumn,
		eventcolumns.UtmSourceColumn,
		eventcolumns.UtmMediumColumn,
		eventcolumns.UtmCampaignColumn,
		eventcolumns.UtmIDColumn,
		eventcolumns.UtmCreativeFormatColumn,
		eventcolumns.ClickIDsGclidColumn,
		eventcolumns.ClickIDsDclidColumn,
		eventcolumns.ClickIDsSrsltidColumn,
		eventcolumns.ClickIDsGbraidColumn,
		eventcolumns.ClickIDsWbraidColumn,
		eventcolumns.ClickIDsFbclidColumn,
		eventcolumns.ClickIDsMsclkidColumn,
	}
}

func sessionColumns() []schema.SessionColumn {
	return []schema.SessionColumn{
		sessioncolumns.SessionIDColumn,
		sessioncolumns.FirstEventTimeColumn,
		sessioncolumns.LastEventTimeColumn,
		sessioncolumns.DurationColumn,
		sessioncolumns.TotalEventsColumn,
		sessioncolumns.ReferrerColumn,
		sessioncolumns.SplitCauseColumn,
		sessioncolumns.SessionSourceColumn,
		sessioncolumns.SessionMediumColumn,
		sessioncolumns.SessionTermColumn,
		sessioncolumns.SessionEntryPageLocationColumn,
		sessioncolumns.SessionSecondPageLocationColumn,
		sessioncolumns.SessionExitPageLocationColumn,
		sessioncolumns.SessionEntryPageTitleColumn,
		sessioncolumns.SessionSecondPageTitleColumn,
		sessioncolumns.SessionExitPageTitleColumn,
		sessioncolumns.SessionUtmCampaignColumn,
		sessioncolumns.SessionUtmSourceColumn,
		sessioncolumns.SessionUtmMediumColumn,
		sessioncolumns.SessionUtmContentColumn,
		sessioncolumns.SessionUtmTermColumn,
		sessioncolumns.SessionUtmIDColumn,
		sessioncolumns.SessionUtmSourcePlatformColumn,
		sessioncolumns.SessionUtmCreativeFormatColumn,
		sessioncolumns.SessionUtmMarketingTacticColumn,
		sessioncolumns.SessionClickIDGclidColumn,
		sessioncolumns.SessionClickIDDclidColumn,
		sessioncolumns.SessionClickIDGbraidColumn,
		sessioncolumns.SessionClickIDSrsltidColumn,
		sessioncolumns.SessionClickIDWbraidColumn,
		sessioncolumns.SessionClickIDFbclidColumn,
		sessioncolumns.SessionClickIDMsclkidColumn,
		sessioncolumns.SessionTotalPageViewsColumn,
		sessioncolumns.SessionUniquePageViewsColumn,
		sessioncolumns.SessionIsBouncedColumn,
	}
}

func sessionScopedEventColumns() []schema.SessionScopedEventColumn {
	return []schema.SessionScopedEventColumn{
		eventcolumns.SSESessionHitNumber,
		eventcolumns.SSESessionPageNumber,
		eventcolumns.SSETimeOnPage,
		eventcolumns.SSEIsEntryPage,
		eventcolumns.SSEIsExitPage,
		eventcolumns.SSEIsBounce,
		eventcolumns.EventPreviousPageLocation,
		eventcolumns.EventNextPageLocation,
		eventcolumns.EventPreviousPageTitle,
		eventcolumns.EventNextPageTitle,
		eventcolumns.SSETrafficFilterName,
	}
}
