// Package columnset provides a default columnset for the tracker API.
package columnset

import (
	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/columns/sessioncolumns"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocolschema"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// columnSetConfig holds the configuration for column set initialization.
type columnSetConfig struct {
	geoColumns    []schema.EventColumn
	deviceColumns []schema.EventColumn
}

// ColumnSetOption is a function that modifies columnSetConfig.
type ColumnSetOption func(*columnSetConfig)

// newDefaultColumnSetConfig returns a columnSetConfig initialized with stub implementations.
func newDefaultColumnSetConfig() *columnSetConfig {
	return &columnSetConfig{
		geoColumns: []schema.EventColumn{
			eventcolumns.GeoContinentStubColumn,
			eventcolumns.GeoCountryStubColumn,
			eventcolumns.GeoRegionStubColumn,
			eventcolumns.GeoCityStubColumn,
			eventcolumns.GeoSubContinentStubColumn,
			eventcolumns.GeoMetroStubColumn,
		},
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

// WithGeoIPColumns returns a ColumnSetOption that sets the geo columns.
func WithGeoIPColumns(cols []schema.EventColumn) ColumnSetOption {
	return func(cfg *columnSetConfig) {
		cfg.geoColumns = cols
	}
}

// WithDeviceDetectionColumns returns a ColumnSetOption that sets the device detection columns.
func WithDeviceDetectionColumns(cols []schema.EventColumn) ColumnSetOption {
	return func(cfg *columnSetConfig) {
		cfg.deviceColumns = cols
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

	// Merge geo and device columns into a single slice for the static registry
	injectedColumns := make([]schema.EventColumn, 0, len(cfg.geoColumns)+len(cfg.deviceColumns))
	injectedColumns = append(injectedColumns, cfg.geoColumns...)
	injectedColumns = append(injectedColumns, cfg.deviceColumns...)

	return schema.NewColumnsMerger([]schema.ColumnsRegistry{
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
	})
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
	}
}

func sessionScopedEventColumns() []schema.SessionScopedEventColumn {
	return []schema.SessionScopedEventColumn{
		eventcolumns.SSESessionHitNumber,
		eventcolumns.SSESessionPageNumber,
		eventcolumns.SSETrafficFilterName,
	}
}
