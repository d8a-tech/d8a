// Package ga4 provides Google Analytics 4 protocol support.
//
//nolint:dupl // Currency conversion functions have similar structure by design
package ga4

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// eventEcommercePurchaseRevenueInUSDColumn creates a new event column that converts the purchase revenue to USD.
func eventEcommercePurchaseRevenueInUSDColumn(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventEcommercePurchaseRevenueInUSD.ID,
		ProtocolInterfaces.EventEcommercePurchaseRevenueInUSD.Field,
		func(event *schema.Event) (any, error) {
			return currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventParamCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventEcommercePurchaseRevenue.Field.Name],
			)
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamCurrency.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamCurrency.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventEcommercePurchaseRevenue.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventEcommercePurchaseRevenue.Version,
			},
		),
		columns.WithEventColumnDocs(
			"Ecommerce Purchase Revenue (USD)",
			"The total purchase revenue converted to USD, calculated from ecommerce_purchase_revenue using the currency converter.", // nolint:lll // it's a description
		),
	)
}

// eventEcommerceRefundValueInUSDColumn creates a new event column that converts the refund value to USD.
func eventEcommerceRefundValueInUSDColumn(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventEcommerceRefundValueInUSD.ID,
		ProtocolInterfaces.EventEcommerceRefundValueInUSD.Field,
		func(event *schema.Event) (any, error) {
			return currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventParamCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventEcommerceRefundValue.Field.Name],
			)
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamCurrency.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamCurrency.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventEcommerceRefundValue.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventEcommerceRefundValue.Version,
			},
		),
		columns.WithEventColumnDocs(
			"Ecommerce Refund Value (USD)",
			"The total refund value converted to USD, calculated from ecommerce_refund_value using the currency converter.",
		),
	)
}

// eventEcommerceShippingValueInUSDColumn creates a new event column that converts the shipping value to USD.
func eventEcommerceShippingValueInUSDColumn(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventEcommerceShippingValueInUSD.ID,
		ProtocolInterfaces.EventEcommerceShippingValueInUSD.Field,
		func(event *schema.Event) (any, error) {
			return currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventParamCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventEcommerceShippingValue.Field.Name],
			)
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamCurrency.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamCurrency.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventEcommerceShippingValue.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventEcommerceShippingValue.Version,
			},
		),
		columns.WithEventColumnDocs(
			"Ecommerce Shipping Value (USD)",
			"The total shipping cost converted to USD, calculated from ecommerce_shipping_value using the currency converter.", // nolint:lll // it's a description
		),
	)
}

// This is taken from parameter
var eventEcommerceTaxValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventEcommerceTaxValue.ID,
	ProtocolInterfaces.EventEcommerceTaxValue.Field,
	"epn.tax",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamTax.ID)),
	columns.WithEventColumnDocs(
		"Tax Value",
		"The ecommerce tax value for the transaction.",
	),
)

// eventEcommerceTaxValueInUSDColumn creates a new event column that converts the tax value to USD.
func eventEcommerceTaxValueInUSDColumn(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventEcommerceTaxValueInUSD.ID,
		ProtocolInterfaces.EventEcommerceTaxValueInUSD.Field,
		func(event *schema.Event) (any, error) {
			return currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventParamCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventEcommerceTaxValue.Field.Name],
			)
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamCurrency.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamCurrency.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventEcommerceTaxValue.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventEcommerceTaxValue.Version,
			},
		),
		columns.WithEventColumnDocs(
			"Tax Value (USD)",
			"The ecommerce tax value converted to USD, calculated from ecommerce_tax_value using the currency converter.",
		),
	)
}
