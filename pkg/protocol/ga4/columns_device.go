package ga4

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/hits"
)

var deviceLanguageColumn = columns.NewLanguageColumn(
	columns.CoreInterfaces.DeviceLanguage.ID,
	columns.CoreInterfaces.DeviceLanguage.Field,
	func(req *hits.ParsedRequest) (string, bool) {
		paramV := req.QueryParams.Get("ul")
		if paramV != "" {
			return paramV, true
		}
		return "", false
	},
	columns.WithEventColumnDocs(
		"Device Language",
		"The language setting of the user's device, extracted from query parameters or Accept-Language header, based on ISO 639 standard for languages and ISO 3166 for country codes (e.g., 'en-us', 'en-gb', 'de-de').", // nolint:lll // it's a description
	),
)
