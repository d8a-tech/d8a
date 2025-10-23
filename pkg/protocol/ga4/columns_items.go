package ga4

import (
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/currency"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var itemsColumn = func(converter currency.Converter) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		ProtocolInterfaces.EventItems.ID,
		ProtocolInterfaces.EventItems.Field,
		func(event *schema.Event) (any, error) {
			var items []any
			for qp, values := range event.BoundHit.QueryParams {
				if strings.HasPrefix(qp, "pr") {
					for _, value := range values {
						if item := parseItem(event, value); item != nil {
							if err := addCurrencyRelatedFields(converter, event, item); err != nil {
								return nil, err
							}
							items = append(items, item)
						}
					}
				}
			}
			return items, nil
		},
		columns.WithEventColumnDocs(
			"Items",
			"Array of ecommerce items associated with the event. Each item contains detailed product information including: item_id, item_name, affiliation, coupon, discount, index, item_brand, item_category (1-5 levels), item_list_id, item_list_name, item_variant, location_id, price, price_in_usd, quantity, item_refund, item_refund_in_usd, item_revenue, item_revenue_in_usd, promotion_id, promotion_name, creative_name, and creative_slot. Used in ecommerce events like purchase, add_to_cart, view_item, etc.", // nolint:lll // it's a description
		),
		columns.WithEventColumnDependsOn(
			// Some props make take values from event itself, so we need to evaluate them before parsing the item
			// Examples: item_list_id, item_list_name
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamItemListID.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamItemListID.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamItemListName.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamItemListName.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamCreativeName.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamCreativeName.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamCreativeSlot.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamCreativeSlot.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamPromotionID.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamPromotionID.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamPromotionName.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamPromotionName.Version,
			},
			// We need to have event_name to calculate the refund value for items
			schema.DependsOnEntry{
				Interface:        columns.CoreInterfaces.EventName.ID,
				GreaterOrEqualTo: columns.CoreInterfaces.EventName.Version,
			},
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventParamCurrency.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventParamCurrency.Version,
			},
		),
	)
}

func addCurrencyRelatedFields(converter currency.Converter, event *schema.Event, item map[string]any) error {
	for _, key := range [][]string{{
		itemKeyPrice,
		itemKeyPriceInUSD,
	}, {
		itemKeyRefund,
		itemKeyRefundInUSD,
	}, {
		itemKeyRevenue,
		itemKeyRevenueInUSD,
	}} {
		if item[key[0]] != nil {
			var err error
			item[key[1]], err = currency.DoConversion(
				converter,
				event.Values[ProtocolInterfaces.EventParamCurrency.Field.Name],
				currency.ISOCurrencyUSD,
				item[key[0]],
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// parseItem parses a GA4 item string in format: id12345~nmProductName~pr99.99~qt1~brBrand~caCategory
func parseItem(event *schema.Event, itemStr string) map[string]any { // nolint:funlen,gocyclo,lll // contains all item params
	if itemStr == "" {
		return nil
	}

	item := make(map[string]any)
	parts := strings.Split(itemStr, "~")

	price := float64(0)
	quantity := float64(0)

	for _, part := range parts {
		if len(part) < 2 {
			continue
		}

		prefix := part[:2]
		value := part[2:]

		switch prefix {
		case "id":
			item[itemKeyID] = value
		case "nm":
			item[itemKeyName] = value
		case "af":
			item[itemKeyAffiliation] = value
		case "cp":
			item[itemKeyCoupon] = value
		case "ds":
			if discount, err := strconv.ParseFloat(value, 64); err == nil {
				item[itemKeyDiscount] = discount
			}
		case "lp":
			if index, err := strconv.ParseFloat(value, 64); err == nil {
				item[itemKeyIndex] = index
			}
		case "br":
			item[itemKeyBrand] = value
		case "ca":
			item[itemKeyCategory] = value
		case "c2":
			item[itemKeyCategory2] = value
		case "c3":
			item[itemKeyCategory3] = value
		case "c4":
			item[itemKeyCategory4] = value
		case "c5":
			item[itemKeyCategory5] = value
		case "li":
			item[itemKeyListID] = value
		case "ln":
			item[itemKeyListName] = value
		case "va":
			item[itemKeyVariant] = value
		case "lo":
			item[itemKeyLocationID] = value
		case "pr":
			if parsedPrice, err := strconv.ParseFloat(value, 64); err == nil {
				price = parsedPrice
			}
		case "qt":
			if parsedQuantity, err := strconv.ParseFloat(value, 64); err == nil {
				quantity = parsedQuantity
			}
		case "cn":
			item[itemKeyCreativeName] = value
		case "cs":
			item[itemKeyCreativeSlot] = value
		case "pi":
			item[itemKeyPromotionID] = value
		case "pn":
			item[itemKeyPromotionName] = value
		}
	}

	item[itemKeyPrice] = price
	item[itemKeyQuantity] = quantity

	if event.Values[columns.CoreInterfaces.EventName.Field.Name] == RefundEventType {
		item[itemKeyRefund] = price * quantity
		item[itemKeyRevenue] = float64(0)
	} else {
		item[itemKeyRevenue] = price * quantity
		item[itemKeyRefund] = float64(0)
	}

	// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
	// "If set, event-level item_list_id is ignored. If not set, event-level item_list_id is used, if present. "
	if item[itemKeyListID] == nil {
		item[itemKeyListID] = event.Values[ProtocolInterfaces.EventParamItemListID.Field.Name]
	}

	// As above
	if item[itemKeyListName] == nil {
		item[itemKeyListName] = event.Values[ProtocolInterfaces.EventParamItemListName.Field.Name]
	}

	// As above
	if item[itemKeyCreativeName] == nil {
		item[itemKeyCreativeName] = event.Values[ProtocolInterfaces.EventParamCreativeName.Field.Name]
	}

	// As above
	if item[itemKeyCreativeSlot] == nil {
		item[itemKeyCreativeSlot] = event.Values[ProtocolInterfaces.EventParamCreativeSlot.Field.Name]
	}

	// As above
	if item[itemKeyPromotionID] == nil {
		item[itemKeyPromotionID] = event.Values[ProtocolInterfaces.EventParamPromotionID.Field.Name]
	}

	// As above
	if item[itemKeyPromotionName] == nil {
		item[itemKeyPromotionName] = event.Values[ProtocolInterfaces.EventParamPromotionName.Field.Name]
	}

	// Only return item if it has at least an ID or name
	// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
	// "One of item_id or item_name is required."
	if item[itemKeyID] != nil || item[itemKeyName] != nil {
		return item
	}
	return nil
}

// ItemValueFunc computes a numeric value for a single item in an event.
// It is used to aggregate values across all items of the event.
type ItemValueFunc[T int64 | float64] func(event *schema.Event, item map[string]any) (T, error)

// NewItemsBasedEventColumn creates an event column that aggregates values
// across items by applying the provided ItemValueFunc to each item and summing.
func NewItemsBasedEventColumn[T int64 | float64](
	interfaceID schema.InterfaceID,
	interfaceField *arrow.Field,
	valueFunc ItemValueFunc[T],
	options ...columns.EventColumnOptions,
) schema.EventColumn {
	options = append(options, columns.WithEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventItems.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventItems.Version,
		},
	))
	return columns.NewSimpleEventColumn(
		interfaceID,
		interfaceField,
		func(event *schema.Event) (any, error) {
			var value T
			items := event.Values[ProtocolInterfaces.EventItems.Field.Name]
			if items == nil {
				return value, nil
			}
			itemsList, ok := items.([]any)
			if !ok {
				return value, nil
			}
			for _, item := range itemsList {
				itemMap, ok := item.(map[string]any)
				if !ok {
					return value, nil
				}
				result, err := valueFunc(event, itemMap)
				if err != nil {
					return value, err
				}
				value += result
			}
			return value, nil
		},
		options...,
	)
}

var eventEcommercePurchaseRevenueColumn = NewItemsBasedEventColumn[float64](
	ProtocolInterfaces.EventEcommercePurchaseRevenue.ID,
	ProtocolInterfaces.EventEcommercePurchaseRevenue.Field,
	func(event *schema.Event, item map[string]any) (float64, error) {
		if event.Values[columns.CoreInterfaces.EventName.Field.Name] == RefundEventType {
			return float64(0), nil
		}
		if item["item_revenue"] == nil {
			return float64(0), nil
		}
		reveAsFloat, ok := item["item_revenue"].(float64)
		if !ok {
			return float64(0), nil
		}
		return reveAsFloat, nil
	},
	columns.WithEventColumnDocs(
		"Ecommerce Purchase Revenue",
		"Total purchase revenue calculated by summing item_revenue across all items in the event. Zero for refund events. Represents the total transaction value from purchased items.", // nolint:lll // it's a description
	),
)

var eventEcommerceRefundValueColumn = NewItemsBasedEventColumn[float64](
	ProtocolInterfaces.EventEcommerceRefundValue.ID,
	ProtocolInterfaces.EventEcommerceRefundValue.Field,
	func(event *schema.Event, item map[string]any) (float64, error) {
		if event.Values[columns.CoreInterfaces.EventName.Field.Name] != RefundEventType {
			return float64(0), nil
		}
		if item["item_refund"] == nil {
			return float64(0), nil
		}
		refundAsFloat, ok := item["item_refund"].(float64)
		if !ok {
			return float64(0), nil
		}
		return refundAsFloat, nil
	},
	columns.WithEventColumnDocs(
		"Ecommerce Refund Value",
		"Total refund value calculated by summing item_refund across all items in the event. Only populated for refund events, zero otherwise. Represents the total value refunded.", // nolint:lll // it's a description
	),
)

var eventEcommerceUniqueItemsColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventEcommerceUniqueItems.ID,
	ProtocolInterfaces.EventEcommerceUniqueItems.Field,
	func(event *schema.Event) (any, error) {

		items := event.Values[ProtocolInterfaces.EventItems.Field.Name]
		if items == nil {
			return int64(0), nil
		}
		itemsList, ok := items.([]any)
		if !ok {
			return int64(0), nil
		}
		uniques := make(map[string]bool)
		for _, item := range itemsList {
			itemMap, ok := item.(map[string]any)
			if !ok {
				return int64(0), nil
			}
			itemID, ok := itemMap["item_id"]
			if ok {
				itemIDStr, ok := itemID.(string)
				if !ok {
					continue
				}
				uniques[itemIDStr] = true
				continue
			}

			itemName, ok := itemMap["item_name"]
			if ok {
				itemNameStr, ok := itemName.(string)
				if !ok {
					continue
				}
				uniques[itemNameStr] = true
				continue
			}
			return int64(0), nil
		}
		return int64(len(uniques)), nil
	},
	columns.WithEventColumnDocs(
		"Ecommerce Unique Items",
		"Count of unique items in the event, determined by distinct item_id or item_name values. Useful for understanding product variety in transactions.", // nolint:lll // it's a description
	),
	columns.WithEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventItems.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventItems.Version,
		},
	),
)

var eventEcommerceItemsTotalQuantityColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventEcommerceItemsTotalQuantity.ID,
	ProtocolInterfaces.EventEcommerceItemsTotalQuantity.Field,
	func(event *schema.Event) (any, error) {
		items := event.Values[ProtocolInterfaces.EventItems.Field.Name]
		if items == nil {
			return int64(0), nil
		}
		itemsList, ok := items.([]any)
		if !ok {
			return int64(0), nil
		}
		totalQuantity := int64(0)
		for _, item := range itemsList {
			itemMap, ok := item.(map[string]any)
			if !ok {
				continue
			}
			q, ok := itemMap[itemKeyQuantity]
			if !ok {
				continue
			}
			qAsInt, ok := q.(float64)
			if !ok {
				continue
			}
			totalQuantity += int64(qAsInt)
		}
		return totalQuantity, nil
	},
	columns.WithEventColumnDocs(
		"Ecommerce Items Total Quantity",
		"Total quantity of all items in the event, calculated by summing the quantity field across all items. Represents the total number of product units in the transaction.", // nolint:lll // it's a description
	),
	columns.WithEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventItems.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventItems.Version,
		},
	),
)
