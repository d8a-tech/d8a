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

var deviceScreenResolutionColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.DeviceScreenResolution.ID,
	columns.CoreInterfaces.DeviceScreenResolution.Field,
	"sr",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(columns.CoreInterfaces.DeviceScreenResolution.ID)),
	),
	columns.WithEventColumnDocs(
		"Device screen resolution",
		"The screen resolution of the user's device (e.g., '1920x1080', '375x667').",
	),
)
