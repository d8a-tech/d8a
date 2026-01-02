package cmd

import (
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/protocol/d8a"
	"github.com/d8a-tech/d8a/pkg/protocol/ga4"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

func protocolFromCMD(cmd *cli.Command) protocol.Protocol {
	protocol := cmd.String(protocolFlag.Name)
	if protocol == "ga4" {
		return ga4.NewGA4Protocol(currencyConverter, propertySource(cmd))
	}
	if protocol == "d8a" {
		return d8a.NewD8AProtocol(currencyConverter, propertySource(cmd))
	}
	logrus.Panicf("invalid protocol: %s, valid values are 'ga4' and 'd8a'", protocol)
	return nil
}
