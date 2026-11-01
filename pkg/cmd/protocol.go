package cmd

import (
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/properties"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/d8a"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/protocol/matomo"
	"github.com/d8a-tech/d8a/pkg/transformations"
	"github.com/urfave/cli/v3"
)

func protocols(
	cmd *cli.Command, converter currency.Converter, settings properties.SettingsRegistry,
) []protocol.Protocol {
	return []protocol.Protocol{
		ga4.NewGA4Protocol(converter, settings),
		d8a.NewD8AProtocol(converter, settings),
		matomo.NewMatomoProtocol(
			matomo.NewFromIDSiteExtractor(settings),
			settings,
			matomo.WithExtraTrackingEndpoints(cmd.StringSlice(matomoTrackingEndpointsFlag.Name)),
		),
	}
}

// transformationsCatalog lists what event transformations can read and write with the
// built-in protocols. The catalog only inspects their columns, so empty settings suffice.
func transformationsCatalog(cmd *cli.Command) *transformations.Catalog {
	settings := properties.NewStaticSettingsRegistry(nil, properties.WithDefaultConfig(&properties.Settings{}))
	return transformations.NewCatalog(protocols(cmd, currency.NewDummyConverter(1), settings)...)
}

func protocolByID(id string, cmd *cli.Command, converter currency.Converter) protocol.Protocol {
	allProtocols := protocols(cmd, converter, propertySettings(cmd))
	for _, protocol := range allProtocols {
		if protocol.ID() == id {
			return protocol
		}
	}
	return nil
}
