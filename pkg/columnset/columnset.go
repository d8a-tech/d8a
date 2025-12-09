// Package columnset provides a default columnset for the tracker API.
package columnset

import (
	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/columns/sessioncolumns"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocolschema"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

func stubGeoIPColumns() []schema.EventColumn {
	return []schema.EventColumn{
		eventcolumns.GeoContinentStubColumn,
		eventcolumns.GeoCountryStubColumn,
		eventcolumns.GeoRegionStubColumn,
		eventcolumns.GeoCityStubColumn,
		eventcolumns.GeoSubContinentStubColumn,
		eventcolumns.GeoMetroStubColumn,
	}
}

// DefaultColumnRegistry returns a default column registry for the tracker API.
func DefaultColumnRegistry(
	theProtocol protocol.Protocol,
	geoColumns []schema.EventColumn,
	psr properties.SettingsRegistry,
) schema.ColumnsRegistry {
	if len(geoColumns) == 0 {
		logrus.Info("No geo columns provided, using stub implementations")
		geoColumns = stubGeoIPColumns()
	}
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
			schema.NewColumns([]schema.SessionColumn{}, geoColumns, []schema.SessionScopedEventColumn{}),
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
		eventcolumns.SSEIsEntry,
	}
}
