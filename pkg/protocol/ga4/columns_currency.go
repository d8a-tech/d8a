package ga4

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// eventPurchaseRevenueInUSDColumn creates a new event column that converts the purchase revenue to USD.
func eventPurchaseRevenueInUSDColumn(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventPurchaseRevenueInUSD.ID,
		ProtocolInterfaces.EventPurchaseRevenueInUSD.Field,
		func(event *schema.Event) (any, error) {
			return currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventPurchaseRevenue.Field.Name],
			)
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventCurrency.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventCurrency.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventPurchaseRevenue.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventPurchaseRevenue.Version,
			},
		),
	)
}
