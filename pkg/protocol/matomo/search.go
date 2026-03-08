package matomo

import "github.com/d8a-tech/d8a/pkg/columns"

var eventParamsSearchKeywordColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsSearchKeyword.ID,
	ProtocolInterfaces.EventParamsSearchKeyword.Field,
	"search",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsSearchKeyword.ID)),
	),
	columns.WithEventColumnDocs(
		"Search Keyword",
		"The site-search keyword extracted from the search query parameter. "+
			"This is not the external referrer keyword (for example, not referer_keyword).",
	),
)

var eventParamsSearchCategoryColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsSearchCategory.ID,
	ProtocolInterfaces.EventParamsSearchCategory.Field,
	"search_cat",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsSearchCategory.ID)),
	),
	columns.WithEventColumnDocs(
		"Search Category",
		"The search category used in a site search, extracted from the search_cat query parameter.",
	),
)

var eventParamsSearchCountColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsSearchCount.ID,
	ProtocolInterfaces.EventParamsSearchCount.Field,
	"search_count",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(columns.CastToInt64OrNil(ProtocolInterfaces.EventParamsSearchCount.ID)),
	columns.WithEventColumnDocs(
		"Search Count",
		"The number of search results displayed, extracted from the search_count query parameter.",
	),
)
