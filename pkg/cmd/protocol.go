package cmd

import (
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/d8a"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/urfave/cli/v3"
)

func protocols(cmd *cli.Command) []protocol.Protocol {
	return []protocol.Protocol{
		ga4.NewGA4Protocol(currencyConverter, propertySettings(cmd)),
		d8a.NewD8AProtocol(currencyConverter, propertySettings(cmd)),
	}
}

func protocolByID(id string, cmd *cli.Command) protocol.Protocol {
	allProtocols := protocols(cmd)
	for _, protocol := range allProtocols {
		if protocol.ID() == id {
			return protocol
		}
	}
	return nil
}
