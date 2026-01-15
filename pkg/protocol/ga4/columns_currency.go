// Package ga4 provides Google Analytics 4 protocol support.
//
//nolint:dupl // Currency conversion functions have similar structure by design
package ga4

import (
	"fmt"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// eventEcommercePurchaseRevenueInUSDColumn creates a new event column that converts the purchase revenue to USD.
func eventEcommercePurchaseRevenueInUSDColumn(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventEcommercePurchaseRevenueInUSD.ID,
		ProtocolInterfaces.EventEcommercePurchaseRevenueInUSD.Field,
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			conversion, err := currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventParamCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventEcommercePurchaseRevenue.Field.Name],
			)
			if err != nil {
				return nil, schema.NewRetryableError(fmt.Sprintf("failed to convert purchase revenue to USD: %s", err))
			}
			return conversion, nil
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface: ProtocolInterfaces.EventParamCurrency.ID,
			},
			schema.DependsOnEntry{
				Interface: ProtocolInterfaces.EventEcommercePurchaseRevenue.ID,
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
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			conversion, err := currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventParamCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventEcommerceRefundValue.Field.Name],
			)
			if err != nil {
				return nil, schema.NewRetryableError(fmt.Sprintf("failed to convert refund value to USD: %s", err))
			}
			return conversion, nil
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface: ProtocolInterfaces.EventParamCurrency.ID,
			},
			schema.DependsOnEntry{
				Interface: ProtocolInterfaces.EventEcommerceRefundValue.ID,
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
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			conversion, err := currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventParamCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventEcommerceShippingValue.Field.Name],
			)
			if err != nil {
				return nil, schema.NewRetryableError(fmt.Sprintf("failed to convert shipping value to USD: %s", err))
			}
			return conversion, nil
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface: ProtocolInterfaces.EventParamCurrency.ID,
			},
			schema.DependsOnEntry{
				Interface: ProtocolInterfaces.EventEcommerceShippingValue.ID,
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
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			conversion, err := currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventParamCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				event.Values[ProtocolInterfaces.EventEcommerceTaxValue.Field.Name],
			)
			if err != nil {
				return nil, schema.NewRetryableError(fmt.Sprintf("failed to convert tax value to USD: %s", err))
			}
			return conversion, nil
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface: ProtocolInterfaces.EventParamCurrency.ID,
			},
			schema.DependsOnEntry{
				Interface: ProtocolInterfaces.EventEcommerceTaxValue.ID,
			},
		),
		columns.WithEventColumnDocs(
			"Tax Value (USD)",
			"The ecommerce tax value converted to USD, calculated from ecommerce_tax_value using the currency converter.",
		),
	)
}
