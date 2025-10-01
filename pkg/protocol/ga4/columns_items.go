package ga4

import (
	"strconv"
	"strings"

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
			item["item_id"] = value
		case "nm":
			item["item_name"] = value
		case "af":
			item["affiliation"] = value
		case "cp":
			item["coupon"] = value
		case "ds":
			if discount, err := strconv.ParseFloat(value, 64); err == nil {
				item["discount"] = discount
			}
		case "lp":
			if index, err := strconv.ParseFloat(value, 64); err == nil {
				item["index"] = index
			}
		case "br":
			item["item_brand"] = value
		case "ca":
			item["item_category"] = value
		case "c2":
			item["item_category2"] = value
		case "c3":
			item["item_category3"] = value
		case "c4":
			item["item_category4"] = value
		case "c5":
			item["item_category5"] = value
		case "li":
			item["item_list_id"] = value
		case "ln":
			item["item_list_name"] = value
		case "va":
			item["item_variant"] = value
		case "lo":
			item["location_id"] = value
		case "pr":
			if parsedPrice, err := strconv.ParseFloat(value, 64); err == nil {
				price = parsedPrice
			}
		case "qt":
			if parsedQuantity, err := strconv.ParseFloat(value, 64); err == nil {
				quantity = parsedQuantity
			}
		case "cn":
			item["creative_name"] = value
		case "cs":
			item["creative_slot"] = value
		case "pi":
			item["promotion_id"] = value
		case "pn":
			item["promotion_name"] = value
		}
	}

	item["price"] = price
	item["quantity"] = quantity

	if event.Values[columns.CoreInterfaces.EventName.Field.Name] == RefundEventType {
		item["item_refund"] = price * quantity
		item["item_revenue"] = float64(0)
	} else {
		item["item_revenue"] = price * quantity
		item["item_refund"] = float64(0)
	}

	// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
	// "If set, event-level item_list_id is ignored. If not set, event-level item_list_id is used, if present. "
	if item["item_list_id"] == nil {
		item["item_list_id"] = event.Values[ProtocolInterfaces.EventItemListID.Field.Name]
	}

	// As above
	if item["item_list_name"] == nil {
		item["item_list_name"] = event.Values[ProtocolInterfaces.EventItemListName.Field.Name]
	}

	// As above
	if item["creative_name"] == nil {
		item["creative_name"] = event.Values[ProtocolInterfaces.EventCreativeName.Field.Name]
	}

	// As above
	if item["creative_slot"] == nil {
		item["creative_slot"] = event.Values[ProtocolInterfaces.EventCreativeSlot.Field.Name]
	}

	// As above
	if item["promotion_id"] == nil {
		item["promotion_id"] = event.Values[ProtocolInterfaces.EventPromotionID.Field.Name]
	}

	// As above
	if item["promotion_name"] == nil {
		item["promotion_name"] = event.Values[ProtocolInterfaces.EventPromotionName.Field.Name]
	}

	// Only return item if it has at least an ID or name
	// https://developers.google.com/analytics/devguides/collection/ga4/reference/events?client_type=gtag#add_payment_info
	// "One of item_id or item_name is required."
	if item["item_id"] != nil || item["item_name"] != nil {
		return item
	}
	return nil
}
