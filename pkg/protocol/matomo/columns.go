//nolint:godox // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/schema"
)

const pageViewEventType = "page_view"

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
}
