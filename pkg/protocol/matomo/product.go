package matomo

import (
	"encoding/json"
	"fmt"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var eventEcommercePurchaseRevenueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventEcommercePurchaseRevenue.ID,
	ProtocolInterfaces.EventEcommercePurchaseRevenue.Field,
	"revenue",
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventEcommercePurchaseRevenue.ID)),
	columns.WithEventColumnDocs(
		"Ecommerce Purchase Revenue",
		"The grand total for the ecommerce order, extracted from the revenue query parameter.",
	),
)

var eventEcommerceShippingValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventEcommerceShippingValue.ID,
	ProtocolInterfaces.EventEcommerceShippingValue.Field,
	"ec_sh",
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventEcommerceShippingValue.ID)),
	columns.WithEventColumnDocs(
		"Ecommerce Shipping Value",
		"The shipping cost of the ecommerce order, extracted from the ec_sh query parameter.",
	),
)

var eventEcommerceSubtotalValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventEcommerceSubtotalValue.ID,
	ProtocolInterfaces.EventEcommerceSubtotalValue.Field,
	"ec_st",
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventEcommerceSubtotalValue.ID)),
	columns.WithEventColumnDocs(
		"Ecommerce Subtotal Value",
		"The subtotal of the ecommerce order excluding shipping, extracted from the ec_st query parameter.",
	),
)

var eventEcommerceTaxValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventEcommerceTaxValue.ID,
	ProtocolInterfaces.EventEcommerceTaxValue.Field,
	"ec_tx",
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventEcommerceTaxValue.ID)),
	columns.WithEventColumnDocs(
		"Ecommerce Tax Value",
		"The tax amount of the ecommerce order, extracted from the ec_tx query parameter.",
	),
)

var eventEcommerceDiscountValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventEcommerceDiscountValue.ID,
	ProtocolInterfaces.EventEcommerceDiscountValue.Field,
	"ec_dt",
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventEcommerceDiscountValue.ID)),
	columns.WithEventColumnDocs(
		"Ecommerce Discount Value",
		"The discount offered for the ecommerce order, extracted from the ec_dt query parameter.",
	),
)

var eventEcommerceOrderIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventEcommerceOrderID.ID,
	ProtocolInterfaces.EventEcommerceOrderID.Field,
	"ec_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventEcommerceOrderID.ID)),
	),
	columns.WithEventColumnDocs(
		"Ecommerce Order ID",
		"The order ID for the ecommerce order, extracted from the ec_id query parameter.",
	),
)

var eventParamsProductPriceColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsProductPrice.ID,
	ProtocolInterfaces.EventParamsProductPrice.Field,
	"_pkp",
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamsProductPrice.ID)),
	columns.WithEventColumnDocs(
		"Product Price",
		"The price of the product being viewed, extracted from the _pkp query parameter.",
	),
)

var eventParamsProductSKUColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsProductSKU.ID,
	ProtocolInterfaces.EventParamsProductSKU.Field,
	"_pks",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsProductSKU.ID)),
	),
	columns.WithEventColumnDocs(
		"Product SKU",
		"The SKU of the product being viewed, extracted from the _pks query parameter.",
	),
)

var eventParamsProductNameColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsProductName.ID,
	ProtocolInterfaces.EventParamsProductName.Field,
	"_pkn",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsProductName.ID)),
	),
	columns.WithEventColumnDocs(
		"Product Name",
		"The name of the product being viewed, extracted from the _pkn query parameter.",
	),
)

var eventParamsProductCategory1Column = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventParamsProductCategory1.ID,
	ProtocolInterfaces.EventParamsProductCategory1.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		categories := parseProductCategories(event.BoundHit.MustParsedRequest().QueryParams.Get("_pkc"))
		if len(categories) < 1 {
			return nil, nil //nolint:nilnil // optional field
		}

		return categories[0], nil
	},
	columns.WithEventColumnDocs(
		"Product Category 1",
		"The first product category, extracted from the _pkc query parameter.",
	),
)

var eventParamsProductCategory2Column = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventParamsProductCategory2.ID,
	ProtocolInterfaces.EventParamsProductCategory2.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		categories := parseProductCategories(event.BoundHit.MustParsedRequest().QueryParams.Get("_pkc"))
		if len(categories) < 2 {
			return nil, nil //nolint:nilnil // optional field
		}

		return categories[1], nil
	},
	columns.WithEventColumnDocs(
		"Product Category 2",
		"The second product category, extracted from the _pkc query parameter.",
	),
)

var eventParamsProductCategory3Column = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventParamsProductCategory3.ID,
	ProtocolInterfaces.EventParamsProductCategory3.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		categories := parseProductCategories(event.BoundHit.MustParsedRequest().QueryParams.Get("_pkc"))
		if len(categories) < 3 {
			return nil, nil //nolint:nilnil // optional field
		}

		return categories[2], nil
	},
	columns.WithEventColumnDocs(
		"Product Category 3",
		"The third product category, extracted from the _pkc query parameter.",
	),
)

var eventParamsProductCategory4Column = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventParamsProductCategory4.ID,
	ProtocolInterfaces.EventParamsProductCategory4.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		categories := parseProductCategories(event.BoundHit.MustParsedRequest().QueryParams.Get("_pkc"))
		if len(categories) < 4 {
			return nil, nil //nolint:nilnil // optional field
		}

		return categories[3], nil
	},
	columns.WithEventColumnDocs(
		"Product Category 4",
		"The fourth product category, extracted from the _pkc query parameter.",
	),
)

var eventParamsProductCategory5Column = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventParamsProductCategory5.ID,
	ProtocolInterfaces.EventParamsProductCategory5.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		categories := parseProductCategories(event.BoundHit.MustParsedRequest().QueryParams.Get("_pkc"))
		if len(categories) < 5 {
			return nil, nil //nolint:nilnil // optional field
		}

		return categories[4], nil
	},
	columns.WithEventColumnDocs(
		"Product Category 5",
		"The fifth product category, extracted from the _pkc query parameter.",
	),
)

func parseProductCategories(raw string) []string {
	if raw == "" {
		return nil
	}

	var categories []string
	if err := json.Unmarshal([]byte(raw), &categories); err == nil {
		if len(categories) == 0 {
			return nil
		}

		return categories
	}

	return []string{raw}
}

func parseEcommerceItems(raw string) ([]map[string]any, error) {
	if raw == "" {
		return nil, nil
	}

	var items []any
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil, fmt.Errorf("parsing ec_items JSON: %w", err)
	}

	var result []map[string]any
	for _, item := range items {
		row := itemToRow(item)
		if row != nil {
			result = append(result, row)
		}
	}

	return result, nil
}

func itemToRow(item any) map[string]any {
	itemArray, ok := item.([]any)
	if !ok || len(itemArray) == 0 {
		return nil
	}

	sku := coerceSKU(itemArray[0])
	if sku == "" {
		return nil
	}

	row := map[string]any{
		ecommerceSKU: sku,
	}

	// Slot 1: name
	row[ecommerceName] = getStringOrDefault(getItemSlot(itemArray, 1), "")

	// Slot 2: category
	addCategoriesToRow(row, getItemSlot(itemArray, 2))

	// Slot 3: price
	row[ecommercePrice] = getFloat64OrDefault(getItemSlot(itemArray, 3), 0.0)

	// Slot 4: quantity
	row[ecommerceQuantity] = getFloat64OrDefault(getItemSlot(itemArray, 4), 1.0)

	return row
}

func coerceSKU(val any) string {
	if strVal, ok := val.(string); ok {
		return strVal
	}

	switch v := val.(type) {
	case float64:
		return fmt.Sprintf("%g", v)
	case bool:
		return fmt.Sprintf("%v", v)
	}

	return ""
}

func getItemSlot(itemArray []any, index int) any {
	if index < len(itemArray) {
		return itemArray[index]
	}
	return nil
}

func getStringOrDefault(val any, def string) string {
	if val == nil {
		return def
	}

	if str, ok := val.(string); ok {
		return str
	}

	return def
}

func getFloat64OrDefault(val any, def float64) float64 {
	if val == nil {
		return def
	}

	if num, ok := val.(float64); ok {
		return num
	}

	return def
}

func addCategoriesToRow(row map[string]any, val any) {
	if val == nil {
		return
	}

	categories := parseEcommerceItemCategory(val)
	if categories[0] != nil {
		row[ecommerceCategory1] = categories[0]
	}
	if categories[1] != nil {
		row[ecommerceCategory2] = categories[1]
	}
	if categories[2] != nil {
		row[ecommerceCategory3] = categories[2]
	}
	if categories[3] != nil {
		row[ecommerceCategory4] = categories[3]
	}
	if categories[4] != nil {
		row[ecommerceCategory5] = categories[4]
	}
}

func parseEcommerceItemCategory(raw any) [5]any {
	var result [5]any

	// Try parsing as string first
	if strVal, ok := raw.(string); ok {
		if strVal != "" {
			result[0] = strVal
		}
		return result
	}

	// Try parsing as []any array
	if arr, ok := raw.([]any); ok {
		for i, v := range arr {
			if i >= 5 {
				break
			}
			if str, ok := v.(string); ok && str != "" {
				result[i] = str //nolint:gosec // array bounds are checked above
			}
		}
		return result
	}

	// Try parsing as []string array (decoded string slice)
	if arrStr, ok := raw.([]string); ok {
		for i, v := range arrStr {
			if i >= 5 {
				break
			}
			if v != "" {
				result[i] = v //nolint:gosec // array bounds are checked above
			}
		}
		return result
	}

	return result
}
