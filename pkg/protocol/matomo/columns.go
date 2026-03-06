//nolint:godox // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

const (
	pageViewEventType   = "page_view"
	downloadEventType   = "download"
	outlinkEventType    = "outlink"
	siteSearchEventType = "site_search"
	ecOrderEventType    = "ecommerce_order"
	videoPlayEventType  = "video_play"
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
	eventParamsCategoryColumn,
	eventParamsActionColumn,
	eventParamsValueColumn,
	eventParamsContentInteractionColumn,
	eventParamsContentNameColumn,
	eventParamsContentPieceColumn,
	eventParamsContentTargetColumn,
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
	sessionTotalScrollsColumn,
	sessionTotalOutboundClicksColumn,
	sessionUniqueOutboundClicksColumn,
	sessionTotalSiteSearchesColumn,
	sessionUniqueSiteSearchesColumn,
	sessionTotalFormInteractionsColumn,
	sessionUniqueFormInteractionsColumn,
	sessionTotalVideoEngagementsColumn,
	sessionTotalFileDownloadsColumn,
	sessionUniqueFileDownloadsColumn,
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

var eventParamsCategoryColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsCategory.ID,
	ProtocolInterfaces.EventParamsCategory.Field,
	"e_c",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnCast(
		columns.StrNilIfErrorOrEmpty(columns.CastToString(ProtocolInterfaces.EventParamsCategory.ID)),
	),
	columns.WithEventColumnDocs(
		"Event Params Category",
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
		"Event Params Action",
		"The action of the event, extracted from the e_a query parameter.",
	),
)

var eventParamsValueColumn = columns.FromQueryParamEventColumn(
	ProtocolInterfaces.EventParamsValue.ID,
	ProtocolInterfaces.EventParamsValue.Field,
	"e_v",
	columns.WithEventColumnCast(columns.CastToFloat64OrNil(ProtocolInterfaces.EventParamsValue.ID)),
	columns.WithEventColumnDocs(
		"Event Params Value",
		"The numeric value of the event, extracted from the e_v query parameter.",
	),
)
