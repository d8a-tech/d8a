// Package ga4 provides Google Analytics 4 protocol support.
//
//nolint:dupl // Currency conversion functions have similar structure by design
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

// eventRefundValueInUSDColumn creates a new event column that converts the refund value to USD.
func eventRefundValueInUSDColumn(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventRefundValueInUSD.ID,
		ProtocolInterfaces.EventRefundValueInUSD.Field,
		func(event *schema.Event) (any, error) {
			return currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventRefundValue.Field.Name],
			)
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventCurrency.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventCurrency.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventRefundValue.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventRefundValue.Version,
			},
		),
	)
}

// eventShippingValueInUSDColumn creates a new event column that converts the shipping value to USD.
func eventShippingValueInUSDColumn(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventShippingValueInUSD.ID,
		ProtocolInterfaces.EventShippingValueInUSD.Field,
		func(event *schema.Event) (any, error) {
			return currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventShippingValue.Field.Name],
			)
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventCurrency.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventCurrency.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventShippingValue.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventShippingValue.Version,
			},
		),
	)
}

// eventTaxValueInUSDColumn creates a new event column that converts the tax value to USD.
func eventTaxValueInUSDColumn(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventTaxValueInUSD.ID,
		ProtocolInterfaces.EventTaxValueInUSD.Field,
		func(event *schema.Event) (any, error) {
			return currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventTax.Field.Name],
			)
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventCurrency.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventCurrency.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventTax.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventTax.Version,
			},
		),
	)
}
