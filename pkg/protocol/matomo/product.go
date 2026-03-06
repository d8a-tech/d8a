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
