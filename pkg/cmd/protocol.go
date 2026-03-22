package cmd

import (
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/d8a"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/d8a-tech/d8a/pkg/protocol/matomo"
	"github.com/urfave/cli/v3"
)

func protocols(cmd *cli.Command, converter currency.Converter) []protocol.Protocol {
	return []protocol.Protocol{
		ga4.NewGA4Protocol(converter, propertySettings(cmd)),
		d8a.NewD8AProtocol(converter, propertySettings(cmd)),
		matomo.NewMatomoProtocol(
			matomo.NewFromIDSiteExtractor(propertySettings(cmd)),
			matomo.WithExtraTrackingEndpoints(cmd.StringSlice(matomoTrackingEndpointsFlag.Name)),
		),
	}
}

func protocolByID(id string, cmd *cli.Command, converter currency.Converter) protocol.Protocol {
	allProtocols := protocols(cmd, converter)
	for _, protocol := range allProtocols {
		if protocol.ID() == id {
			return protocol
		}
	}
	return nil
}
