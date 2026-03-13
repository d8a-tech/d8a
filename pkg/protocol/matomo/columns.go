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
	customEventType         = "custom_event"
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
	eventMeasurementIDColumn,
	eventParamsPageViewIDColumn,
	eventParamsGoalIDColumn,
	eventParamsCategoryColumn,
	eventParamsActionColumn,
	eventParamsValueColumn,
	eventParamsMediaAssetIDColumn,
	eventParamsMediaTypeColumn,
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
	eventEcommerceOrderIDColumn,
	eventEcommerceItemsColumn,
	eventEcommerceItemsTotalQuantityColumn,
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
