package matomo

import (
	"encoding/json"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
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

var eventParamsProductCategoriesColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventParamsProductCategories.ID,
	ProtocolInterfaces.EventParamsProductCategories.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		category := event.BoundHit.MustParsedRequest().QueryParams.Get("_pkc")
		normalized := normalizeProductCategories(category)
		if normalized == nil {
			return nil, nil //nolint:nilnil // optional field
		}

		return normalized, nil
	},
	columns.WithEventColumnDocs(
		"Product Categories",
		"The product categories, extracted from the _pkc query parameter.",
	),
)

func normalizeProductCategories(raw string) []any {
	if raw == "" {
		return nil
	}

	var categories []string
	if err := json.Unmarshal([]byte(raw), &categories); err == nil {
		if len(categories) == 0 {
			return nil
		}

		normalized := make([]any, 0, len(categories))
		for _, category := range categories {
			normalized = append(normalized, map[string]any{"name": category})
		}

		return normalized
	}

	return []any{map[string]any{"name": raw}}
}
