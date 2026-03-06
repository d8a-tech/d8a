// Package matomo provides Matomo protocol specific column definitions.
package matomo

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// ProtocolInterfaces are the columns specific to the Matomo protocol.
var ProtocolInterfaces = struct {
	EventLinkURL                  schema.Interface
	EventDownloadURL              schema.Interface
	EventSearchTerm               schema.Interface
	EventParamsCategory           schema.Interface
	EventParamsAction             schema.Interface
	EventParamsValue              schema.Interface
	EventParamsContentInteraction schema.Interface
	EventParamsContentName        schema.Interface
	EventParamsContentPiece       schema.Interface
	EventParamsContentTarget      schema.Interface
	EventParamsProductPrice       schema.Interface
	EventParamsProductSKU         schema.Interface
	EventParamsProductName        schema.Interface
	EventParamsProductCategories  schema.Interface
	EventPreviousPageLocation     schema.Interface
	EventNextPageLocation         schema.Interface
	EventPreviousPageTitle        schema.Interface
	EventNextPageTitle            schema.Interface
}{
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
	EventParamsProductCategories: schema.Interface{
		ID: "matomo.protocols.d8a.tech/event/params_product_categories",
		Field: &arrow.Field{
			Name:     "params_product_categories",
			Type:     arrow.ListOf(arrow.StructOf(arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true})),
			Nullable: true,
		},
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
}
