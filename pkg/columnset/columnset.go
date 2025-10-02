// Package columnset provides a default columnset for the tracker API.
package columnset

import (
	"github.com/d8a-tech/d8a/pkg/columns/eventcolumns"
	"github.com/d8a-tech/d8a/pkg/columns/sessioncolumns"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocolschema"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// DefaultColumnRegistry returns a default column registry for the tracker API.
func DefaultColumnRegistry(theProtocol protocol.Protocol) schema.ColumnsRegistry {
	return schema.NewColumnsMerger([]schema.ColumnsRegistry{
		schema.NewStaticColumnsRegistry(
			map[string]schema.Columns{},
			schema.NewColumns(sessionColumns(), eventColumns()),
		),
		protocolschema.NewFromProtocolColumnsRegistry(protocol.NewStaticRegistry(
			map[string]protocol.Protocol{},
			theProtocol,
		)),
	})
}

func eventColumns() []schema.EventColumn {
	return []schema.EventColumn{
		eventcolumns.EventIDColumn,
		eventcolumns.IPAddressColumn,
		eventcolumns.ClientIDColumn,
		eventcolumns.UserIDColumn,
		eventcolumns.DateColumn,
		eventcolumns.PropertyIDColumn,
		eventcolumns.TimestampColumn,
		eventcolumns.GclidColumn,
		eventcolumns.DclidColumn,
		eventcolumns.SrsltidColumn,
		eventcolumns.AclidColumn,
		eventcolumns.AnidColumn,
		eventcolumns.UtmMarketingTacticColumn,
		eventcolumns.UtmSourcePlatformColumn,
		eventcolumns.UtmTermColumn,
		eventcolumns.UtmContentColumn,
		eventcolumns.UtmSourceColumn,
		eventcolumns.UtmMediumColumn,
		eventcolumns.UtmCampaignColumn,
		eventcolumns.UtmIDColumn,
		eventcolumns.UtmCreativeFormatColumn,
	}
}

func sessionColumns() []schema.SessionColumn {
	return []schema.SessionColumn{
		sessioncolumns.SessionIDColumn,
		sessioncolumns.FirstEventTimeColumn,
		sessioncolumns.LastEventTimeColumn,
		sessioncolumns.DurationColumn,
		sessioncolumns.TotalEventsColumn,
	}
}
