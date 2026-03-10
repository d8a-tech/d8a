// Package matomo provides Matomo protocol specific column definitions.
package matomo

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// Ecommerce item child field names for centralized reuse in parser and tests.
const (
	ecommerceSKU       = "sku"
	ecommerceName      = "name"
	ecommerceCategory1 = "category_1"
	ecommerceCategory2 = "category_2"
	ecommerceCategory3 = "category_3"
	ecommerceCategory4 = "category_4"
	ecommerceCategory5 = "category_5"
	ecommercePrice     = "price"
	ecommerceQuantity  = "quantity"
)

// ProtocolInterfaces are the columns specific to the Matomo protocol.
var ProtocolInterfaces = struct {
	EventCustomVariables            schema.Interface
	EventCustomDimensions           schema.Interface
	SessionCustomVariables          schema.Interface
	SessionCustomDimensions         schema.Interface
	EventLinkURL                    schema.Interface
	EventDownloadURL                schema.Interface
	EventSearchTerm                 schema.Interface
	EventParamsCategory             schema.Interface
	EventParamsAction               schema.Interface
	EventParamsValue                schema.Interface
	EventParamsContentInteraction   schema.Interface
	EventParamsContentName          schema.Interface
	EventParamsContentPiece         schema.Interface
	EventParamsContentTarget        schema.Interface
	EventEcommercePurchaseRevenue   schema.Interface
	EventEcommerceShippingValue     schema.Interface
	EventEcommerceSubtotalValue     schema.Interface
	EventEcommerceTaxValue          schema.Interface
	EventEcommerceDiscountValue     schema.Interface
	EventEcommerceOrderID           schema.Interface
	EventEcommerceItems             schema.Interface
	EventParamsProductPrice         schema.Interface
	EventParamsProductSKU           schema.Interface
	EventParamsProductName          schema.Interface
	EventParamsProductCategory1     schema.Interface
	EventParamsProductCategory2     schema.Interface
	EventParamsProductCategory3     schema.Interface
	EventParamsProductCategory4     schema.Interface
	EventParamsProductCategory5     schema.Interface
	EventPreviousPageLocation       schema.Interface
	EventNextPageLocation           schema.Interface
	EventPreviousPageTitle          schema.Interface
	EventNextPageTitle              schema.Interface
	EventParamsPageViewID           schema.Interface
	EventParamsGoalID               schema.Interface
	EventParamsSearchKeyword        schema.Interface
	EventParamsSearchCategory       schema.Interface
	EventParamsSearchCount          schema.Interface
	SessionTotalGoalConversions     schema.Interface
	SessionTotalContentImpressions  schema.Interface
	SessionTotalContentInteractions schema.Interface
	SessionReturningUser            schema.Interface
}{
	EventCustomVariables: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/custom_variables",
		Field: repeatedNameValueField("custom_variables"),
	},
	EventCustomDimensions: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/custom_dimensions",
		Field: repeatedSlotValueField("custom_dimensions"),
	},
	SessionCustomVariables: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/session/session_custom_variables",
		Field: repeatedNameValueField("session_custom_variables"),
	},
	SessionCustomDimensions: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/session/session_custom_dimensions",
		Field: repeatedSlotValueField("session_custom_dimensions"),
	},
	EventLinkURL: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/link_url",
		Field: &arrow.Field{Name: "link_url", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventDownloadURL: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/download_url",
		Field: &arrow.Field{Name: "download_url", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSearchTerm: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/search_term",
		Field: &arrow.Field{Name: "search_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsCategory: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_category",
		Field: &arrow.Field{Name: "params_category", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsAction: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_action",
		Field: &arrow.Field{Name: "params_action", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsValue: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_value",
		Field: &arrow.Field{Name: "params_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamsContentInteraction: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_content_interaction",
		Field: &arrow.Field{Name: "params_content_interaction", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsContentName: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_content_name",
		Field: &arrow.Field{Name: "params_content_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsContentPiece: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_content_piece",
		Field: &arrow.Field{Name: "params_content_piece", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsContentTarget: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_content_target",
		Field: &arrow.Field{Name: "params_content_target", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventEcommercePurchaseRevenue: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/ecommerce_purchase_revenue",
		Field: &arrow.Field{Name: "ecommerce_purchase_revenue", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceShippingValue: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/ecommerce_shipping_value",
		Field: &arrow.Field{Name: "ecommerce_shipping_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceSubtotalValue: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/ecommerce_subtotal_value",
		Field: &arrow.Field{Name: "ecommerce_subtotal_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceTaxValue: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/ecommerce_tax_value",
		Field: &arrow.Field{Name: "ecommerce_tax_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceDiscountValue: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/ecommerce_discount_value",
		Field: &arrow.Field{Name: "ecommerce_discount_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventEcommerceOrderID: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/ecommerce_order_id",
		Field: &arrow.Field{Name: "ecommerce_order_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventEcommerceItems: schema.Interface{
		ID: "matomo.protocols.d8a.tech/event/ecommerce_items",
		Field: &arrow.Field{
			Nullable: true,
			Name:     "ecommerce_items",
			Type: arrow.ListOf(arrow.StructOf(
				arrow.Field{Name: ecommerceSKU, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: ecommerceName, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: ecommerceCategory1, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: ecommerceCategory2, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: ecommerceCategory3, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: ecommerceCategory4, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: ecommerceCategory5, Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: ecommercePrice, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				arrow.Field{Name: ecommerceQuantity, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			)),
		},
	},
	EventParamsProductPrice: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_product_price",
		Field: &arrow.Field{Name: "params_product_price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	},
	EventParamsProductSKU: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_product_sku",
		Field: &arrow.Field{Name: "params_product_sku", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsProductName: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_product_name",
		Field: &arrow.Field{Name: "params_product_name", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsProductCategory1: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_product_category_1",
		Field: &arrow.Field{Name: "params_product_category_1", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsProductCategory2: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_product_category_2",
		Field: &arrow.Field{Name: "params_product_category_2", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsProductCategory3: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_product_category_3",
		Field: &arrow.Field{Name: "params_product_category_3", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsProductCategory4: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_product_category_4",
		Field: &arrow.Field{Name: "params_product_category_4", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsProductCategory5: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_product_category_5",
		Field: &arrow.Field{Name: "params_product_category_5", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPreviousPageLocation: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/previous_page_location",
		Field: &arrow.Field{Name: "previous_page_location", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventNextPageLocation: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/next_page_location",
		Field: &arrow.Field{Name: "next_page_location", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPreviousPageTitle: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/previous_page_title",
		Field: &arrow.Field{Name: "previous_page_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventNextPageTitle: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/next_page_title",
		Field: &arrow.Field{Name: "next_page_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsPageViewID: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_page_view_id",
		Field: &arrow.Field{Name: "params_page_view_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsGoalID: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_goal_id",
		Field: &arrow.Field{Name: "params_goal_id", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsSearchKeyword: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_search_keyword",
		Field: &arrow.Field{Name: "params_search_keyword", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsSearchCategory: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_search_category",
		Field: &arrow.Field{Name: "params_search_category", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventParamsSearchCount: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/params_search_count",
		Field: &arrow.Field{Name: "params_search_count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalGoalConversions: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/session/total_goal_conversions",
		Field: &arrow.Field{Name: "session_total_goal_conversions", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalContentImpressions: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/session/total_content_impressions",
		Field: &arrow.Field{Name: "session_total_content_impressions", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionTotalContentInteractions: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/session/total_content_interactions",
		Field: &arrow.Field{Name: "session_total_content_interactions", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
	SessionReturningUser: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/session/returning_user",
		Field: &arrow.Field{Name: "session_returning_user", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	},
}
