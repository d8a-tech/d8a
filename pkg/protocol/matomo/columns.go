//nolint:godox // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

const (
	pageViewEventType       = "page_view"
	downloadEventType       = "download"
	outlinkEventType        = "outlink"
	siteSearchEventType     = "site_search"
	ecOrderEventType        = "ecommerce_order"
	goalConversionEventType = "goal_conversion"
	contentImpressionType   = "content_impression"
	contentInteractionType  = "content_interaction"
	videoPlayEventType      = "video_play"
)

func isPageViewEvent(event *schema.Event) bool {
	eventName, ok := event.Values[columns.CoreInterfaces.EventName.Field.Name]
	if !ok {
		return false
	}
	eventNameStr, ok := eventName.(string)
	if !ok {
		return false
	}
	return eventNameStr == pageViewEventType
}

var eventColumns = []schema.EventColumn{
	eventIgnoreReferrerColumn,
	eventDateUTCColumn,
	eventTimestampUTCColumn,
	eventPageReferrerColumn,
	eventPageLocationColumn,
	eventPageHostnameColumn,
	eventPagePathColumn,
	eventPageTitleColumn,
	eventTrackingProtocolColumn,
	eventPlatformColumn,
	deviceLanguageColumn,
	eventLinkURLColumn,
	eventDownloadURLColumn,
	eventSearchTermColumn,
	eventParamsPageViewIDColumn,
	eventParamsGoalIDColumn,
	eventParamsCategoryColumn,
	eventParamsActionColumn,
	eventParamsValueColumn,
	eventParamsContentInteractionColumn,
	eventParamsContentNameColumn,
	eventParamsContentPieceColumn,
	eventParamsContentTargetColumn,
	eventParamsSearchKeywordColumn,
	eventParamsSearchCategoryColumn,
	eventParamsSearchCountColumn,
	eventCustomVariablesColumn,
	eventCustomDimensionsColumn,
	eventEcommercePurchaseRevenueColumn,
	eventEcommerceShippingValueColumn,
	eventEcommerceSubtotalValueColumn,
	eventEcommerceTaxValueColumn,
	eventEcommerceDiscountValueColumn,
	eventParamsProductPriceColumn,
	eventParamsProductSKUColumn,
	eventParamsProductNameColumn,
	eventParamsProductCategory1Column,
	eventParamsProductCategory2Column,
	eventParamsProductCategory3Column,
	eventParamsProductCategory4Column,
	eventParamsProductCategory5Column,
}

var sessionColumns = []schema.SessionColumn{
	sessionEntryPageLocationColumn,
	sessionSecondPageLocationColumn,
	sessionExitPageLocationColumn,
	sessionEntryPageTitleColumn,
	sessionSecondPageTitleColumn,
	sessionExitPageTitleColumn,
	sessionUtmCampaignColumn,
	sessionUtmSourceColumn,
	sessionUtmMediumColumn,
	sessionUtmContentColumn,
	sessionUtmTermColumn,
	sessionUtmIDColumn,
	sessionUtmSourcePlatformColumn,
	sessionUtmCreativeFormatColumn,
	sessionUtmMarketingTacticColumn,
	sessionClickIDGclidColumn,
	sessionClickIDDclidColumn,
	sessionClickIDGbraidColumn,
	sessionClickIDSrsltidColumn,
	sessionClickIDWbraidColumn,
	sessionClickIDFbclidColumn,
	sessionClickIDMsclkidColumn,
	sessionTotalPageViewsColumn,
	sessionUniquePageViewsColumn,
	sessionTotalPurchasesColumn,
	sessionTotalGoalConversionsColumn,
	sessionTotalScrollsColumn,
	sessionTotalOutboundClicksColumn,
	sessionUniqueOutboundClicksColumn,
	sessionTotalSiteSearchesColumn,
	sessionTotalContentImpressionsColumn,
	sessionTotalContentInteractionsColumn,
	sessionUniqueSiteSearchesColumn,
	sessionTotalFormInteractionsColumn,
	sessionUniqueFormInteractionsColumn,
	sessionTotalVideoEngagementsColumn,
	sessionTotalFileDownloadsColumn,
	sessionUniqueFileDownloadsColumn,
	sessionReturningUserColumn,
	sessionCustomVariablesColumn,
	sessionCustomDimensionsColumn,
}

var sseColumns = []schema.SessionScopedEventColumn{
	sseTimeOnPageColumn,
	sseIsEntryPageColumn,
	sseIsExitPageColumn,
	eventPreviousPageLocationColumn,
	eventNextPageLocationColumn,
	eventPreviousPageTitleColumn,
	eventNextPageTitleColumn,
}

var eventLinkURLColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventLinkURL.ID,
	ProtocolInterfaces.EventLinkURL.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		v := event.BoundHit.MustParsedRequest().QueryParams.Get("link")
		if v == "" {
			return nil, nil //nolint:nilnil // optional field
		}
		return v, nil
	},
	columns.WithEventColumnDocs(
		"Link URL",
		"The URL of an outbound link clicked by the user, extracted from the link query parameter.", // nolint:lll // it's a description
	),
)

var eventDownloadURLColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventDownloadURL.ID,
	ProtocolInterfaces.EventDownloadURL.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		v := event.BoundHit.MustParsedRequest().QueryParams.Get("download")
		if v == "" {
			return nil, nil //nolint:nilnil // optional field
		}
		return v, nil
	},
	columns.WithEventColumnDocs(
		"Download URL",
		"The URL of a file downloaded by the user, extracted from the download query parameter.", // nolint:lll // it's a description
	),
)

var eventSearchTermColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventSearchTerm.ID,
	ProtocolInterfaces.EventSearchTerm.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		v := event.BoundHit.MustParsedRequest().QueryParams.Get("search")
		if v == "" {
			return nil, nil //nolint:nilnil // optional field
		}
		return v, nil
	},
	columns.WithEventColumnDocs(
		"Search Term",
		"The keyword used in a site search, extracted from the search query parameter.", // nolint:lll // it's a description
	),
)

var eventParamsPageViewIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsPageViewID.ID,
	ProtocolInterfaces.EventParamsPageViewID.Field,
	"pv_id",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsPageViewID.ID)),
	),
	columns.WithEventColumnDocs(
		"Page View ID",
		"The page view identifier, extracted from the pv_id query parameter.",
	),
)

var eventParamsGoalIDColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsGoalID.ID,
	ProtocolInterfaces.EventParamsGoalID.Field,
	"idgoal",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsGoalID.ID)),
	),
	columns.WithEventColumnDocs(
		"Goal ID",
		"The goal identifier, extracted from the idgoal query parameter.",
	),
)

var eventParamsCategoryColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsCategory.ID,
	ProtocolInterfaces.EventParamsCategory.Field,
	"e_c",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsCategory.ID)),
	),
	columns.WithEventColumnDocs(
		"Category",
		"The category of the event, extracted from the e_c query parameter.",
	),
)

var eventParamsActionColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsAction.ID,
	ProtocolInterfaces.EventParamsAction.Field,
	"e_a",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsAction.ID)),
	),
	columns.WithEventColumnDocs(
		"Action",
		"The action of the event, extracted from the e_a query parameter.",
	),
)

var eventParamsValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsValue.ID,
	ProtocolInterfaces.EventParamsValue.Field,
	"e_v",
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamsValue.ID)),
	columns.WithEventColumnDocs(
		"Value",
		"The numeric value of the event, extracted from the e_v query parameter.",
	),
)
