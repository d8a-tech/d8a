package cmd

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/urfave/cli/v3"
)

func buildCurrencyConverter(
	cmd *cli.Command,
) (currency.ManagedConverter, func(), error) {
	converter, err := currency.NewFWAConverter(
		nil,
		currency.WithDestination(cmd.String(currencyDestinationDirectoryFlag.Name)),
		currency.WithInterval(cmd.Duration(currencyRefreshIntervalFlag.Name)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create currency converter: %w", err)
	}

	return converter, func() {}, nil
}
