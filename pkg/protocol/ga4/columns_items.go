package ga4

import (
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var itemsColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventItems.ID,
	ProtocolInterfaces.EventItems.Field,
	func(event *schema.Event) (any, error) {
		var items []any
		for qp, values := range event.BoundHit.QueryParams {
			if strings.HasPrefix(qp, "pr") {
				for _, value := range values {
					if item := parseItem(event, value); item != nil {
						items = append(items, item)
					}
				}
			}
		}
		return items, nil
	},
	columns.WithEventColumnDependsOn(
		// Some props make take values from event itself, so we need to evaluate them before parsing the item
		// Examples: item_list_id, item_list_name
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventItemListID.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventItemListID.Version,
		},
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventItemListName.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventItemListName.Version,
		},
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventCreativeName.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventCreativeName.Version,
		},
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventCreativeSlot.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventCreativeSlot.Version,
		},
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventPromotionID.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventPromotionID.Version,
		},
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventPromotionName.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventPromotionName.Version,
		},
		// We need to have event_name to calculate the refund value for items
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: columns.CoreInterfaces.EventName.Version,
		},
	),
)

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
		item[itemKeyListID] = event.Values[ProtocolInterfaces.EventItemListID.Field.Name]
	}

	// As above
	if item[itemKeyListName] == nil {
		item[itemKeyListName] = event.Values[ProtocolInterfaces.EventItemListName.Field.Name]
	}

	// As above
	if item[itemKeyCreativeName] == nil {
		item[itemKeyCreativeName] = event.Values[ProtocolInterfaces.EventCreativeName.Field.Name]
	}

	// As above
	if item[itemKeyCreativeSlot] == nil {
		item[itemKeyCreativeSlot] = event.Values[ProtocolInterfaces.EventCreativeSlot.Field.Name]
	}

	// As above
	if item[itemKeyPromotionID] == nil {
		item[itemKeyPromotionID] = event.Values[ProtocolInterfaces.EventPromotionID.Field.Name]
	}

	// As above
	if item[itemKeyPromotionName] == nil {
		item[itemKeyPromotionName] = event.Values[ProtocolInterfaces.EventPromotionName.Field.Name]
	}

	// Only return item if it has at least an ID or name
	// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
	// "One of item_id or item_name is required."
	if item[itemKeyID] != nil || item[itemKeyName] != nil {
		return item
	}
	return nil
}

type ItemsBasedIterator[T int64 | float64] func(event *schema.Event, item map[string]any) (T, error)

func NewItemsBasedEventColumn[T int64 | float64](
	interfaceID schema.InterfaceID,
	interfaceField *arrow.Field,
	iterator ItemsBasedIterator[T],
) schema.EventColumn {
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
				result, err := iterator(event, itemMap)
				if err != nil {
					return value, err
				}
				value += result
			}
			return value, nil
		},
		columns.WithEventColumnDependsOn(
			schema.DependsOnEntry{
				Interface:        ProtocolInterfaces.EventItems.ID,
				GreaterOrEqualTo: ProtocolInterfaces.EventItems.Version,
			},
		),
	)
}

var eventPurchaseRevenueColumn = NewItemsBasedEventColumn[float64](
	ProtocolInterfaces.EventPurchaseRevenue.ID,
	ProtocolInterfaces.EventPurchaseRevenue.Field,
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
)

var eventRefundValueColumn = NewItemsBasedEventColumn[float64](
	ProtocolInterfaces.EventRefundValue.ID,
	ProtocolInterfaces.EventRefundValue.Field,
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
)

var eventShippingValueColumn = NewItemsBasedEventColumn[float64](
	ProtocolInterfaces.EventShippingValue.ID,
	ProtocolInterfaces.EventShippingValue.Field,
	func(event *schema.Event, item map[string]any) (float64, error) {
		if item["item_shipping"] == nil {
			return float64(0), nil
		}
		shippingAsFloat, ok := item["item_shipping"].(float64)
		if !ok {
			return float64(0), nil
		}
		return shippingAsFloat, nil
	},
)
var eventUniqueItemsColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventUniqueItems.ID,
	ProtocolInterfaces.EventUniqueItems.Field,
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
			} else {
				return int64(0), nil
			}
		}
		return int64(len(uniques)), nil
	},
	columns.WithEventColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventItems.ID,
			GreaterOrEqualTo: ProtocolInterfaces.EventItems.Version,
		},
	),
)
