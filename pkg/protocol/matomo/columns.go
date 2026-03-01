//nolint:godox // TODO comments are intentional stubs.
package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

type matomoEventIgnoreReferrerColumn struct{}

func (c *matomoEventIgnoreReferrerColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventIgnoreReferrerColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventIgnoreReferrer
}

func (c *matomoEventIgnoreReferrerColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventIgnoreReferrerColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoEventDateUTCColumn struct{}

func (c *matomoEventDateUTCColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventDateUTCColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventDateUTC
}

func (c *matomoEventDateUTCColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventDateUTCColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoEventTimestampUTCColumn struct{}

func (c *matomoEventTimestampUTCColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventTimestampUTCColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventTimestampUTC
}

func (c *matomoEventTimestampUTCColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventTimestampUTCColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoEventPageReferrerColumn struct{}

func (c *matomoEventPageReferrerColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventPageReferrerColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventPageReferrer
}

func (c *matomoEventPageReferrerColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventPageReferrerColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoEventPageTitleColumn struct{}

func (c *matomoEventPageTitleColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventPageTitleColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventPageTitle
}

func (c *matomoEventPageTitleColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventPageTitleColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoEventTrackingProtocolColumn struct{}

func (c *matomoEventTrackingProtocolColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventTrackingProtocolColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventTrackingProtocol
}

func (c *matomoEventTrackingProtocolColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventTrackingProtocolColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoEventPlatformColumn struct{}

func (c *matomoEventPlatformColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventPlatformColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventPlatform
}

func (c *matomoEventPlatformColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventPlatformColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoDeviceLanguageColumn struct{}

func (c *matomoDeviceLanguageColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoDeviceLanguageColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.DeviceLanguage
}

func (c *matomoDeviceLanguageColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoDeviceLanguageColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoEventPageLocationColumn struct{}

func (c *matomoEventPageLocationColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventPageLocationColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventPageLocation
}

func (c *matomoEventPageLocationColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventPageLocationColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoEventPageHostnameColumn struct{}

func (c *matomoEventPageHostnameColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventPageHostnameColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventPageHostname
}

func (c *matomoEventPageHostnameColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventPageHostnameColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoEventPagePathColumn struct{}

func (c *matomoEventPagePathColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoEventPagePathColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.EventPagePath
}

func (c *matomoEventPagePathColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoEventPagePathColumn) Write(event *schema.Event) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSSETimeOnPageColumn struct{}

func (c *matomoSSETimeOnPageColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSSETimeOnPageColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SSETimeOnPage
}

func (c *matomoSSETimeOnPageColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSSETimeOnPageColumn) Write(session *schema.Session, i int) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSSEIsEntryPageColumn struct{}

func (c *matomoSSEIsEntryPageColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSSEIsEntryPageColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SSEIsEntryPage
}

func (c *matomoSSEIsEntryPageColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSSEIsEntryPageColumn) Write(session *schema.Session, i int) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSSEIsExitPageColumn struct{}

func (c *matomoSSEIsExitPageColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSSEIsExitPageColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SSEIsExitPage
}

func (c *matomoSSEIsExitPageColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSSEIsExitPageColumn) Write(session *schema.Session, i int) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionEntryPageLocationColumn struct{}

func (c *matomoSessionEntryPageLocationColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionEntryPageLocationColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionEntryPageLocation
}

func (c *matomoSessionEntryPageLocationColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionEntryPageLocationColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionSecondPageLocationColumn struct{}

func (c *matomoSessionSecondPageLocationColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionSecondPageLocationColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionSecondPageLocation
}

func (c *matomoSessionSecondPageLocationColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionSecondPageLocationColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionExitPageLocationColumn struct{}

func (c *matomoSessionExitPageLocationColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionExitPageLocationColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionExitPageLocation
}

func (c *matomoSessionExitPageLocationColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionExitPageLocationColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionEntryPageTitleColumn struct{}

func (c *matomoSessionEntryPageTitleColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionEntryPageTitleColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionEntryPageTitle
}

func (c *matomoSessionEntryPageTitleColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionEntryPageTitleColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionSecondPageTitleColumn struct{}

func (c *matomoSessionSecondPageTitleColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionSecondPageTitleColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionSecondPageTitle
}

func (c *matomoSessionSecondPageTitleColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionSecondPageTitleColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionExitPageTitleColumn struct{}

func (c *matomoSessionExitPageTitleColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionExitPageTitleColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionExitPageTitle
}

func (c *matomoSessionExitPageTitleColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionExitPageTitleColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUtmCampaignColumn struct{}

func (c *matomoSessionUtmCampaignColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUtmCampaignColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUtmCampaign
}

func (c *matomoSessionUtmCampaignColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUtmCampaignColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUtmSourceColumn struct{}

func (c *matomoSessionUtmSourceColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUtmSourceColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUtmSource
}

func (c *matomoSessionUtmSourceColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUtmSourceColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUtmMediumColumn struct{}

func (c *matomoSessionUtmMediumColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUtmMediumColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUtmMedium
}

func (c *matomoSessionUtmMediumColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUtmMediumColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUtmContentColumn struct{}

func (c *matomoSessionUtmContentColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUtmContentColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUtmContent
}

func (c *matomoSessionUtmContentColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUtmContentColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUtmTermColumn struct{}

func (c *matomoSessionUtmTermColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUtmTermColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUtmTerm
}

func (c *matomoSessionUtmTermColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUtmTermColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUtmIDColumn struct{}

func (c *matomoSessionUtmIDColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUtmIDColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUtmID
}

func (c *matomoSessionUtmIDColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUtmIDColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUtmSourcePlatformColumn struct{}

func (c *matomoSessionUtmSourcePlatformColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUtmSourcePlatformColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUtmSourcePlatform
}

func (c *matomoSessionUtmSourcePlatformColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUtmSourcePlatformColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUtmCreativeFormatColumn struct{}

func (c *matomoSessionUtmCreativeFormatColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUtmCreativeFormatColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUtmCreativeFormat
}

func (c *matomoSessionUtmCreativeFormatColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUtmCreativeFormatColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUtmMarketingTacticColumn struct{}

func (c *matomoSessionUtmMarketingTacticColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUtmMarketingTacticColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUtmMarketingTactic
}

func (c *matomoSessionUtmMarketingTacticColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUtmMarketingTacticColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionClickIDGclidColumn struct{}

func (c *matomoSessionClickIDGclidColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionClickIDGclidColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionClickIDGclid
}

func (c *matomoSessionClickIDGclidColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionClickIDGclidColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionClickIDDclidColumn struct{}

func (c *matomoSessionClickIDDclidColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionClickIDDclidColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionClickIDDclid
}

func (c *matomoSessionClickIDDclidColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionClickIDDclidColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionClickIDGbraidColumn struct{}

func (c *matomoSessionClickIDGbraidColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionClickIDGbraidColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionClickIDGbraid
}

func (c *matomoSessionClickIDGbraidColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionClickIDGbraidColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionClickIDSrsltidColumn struct{}

func (c *matomoSessionClickIDSrsltidColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionClickIDSrsltidColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionClickIDSrsltid
}

func (c *matomoSessionClickIDSrsltidColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionClickIDSrsltidColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionClickIDWbraidColumn struct{}

func (c *matomoSessionClickIDWbraidColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionClickIDWbraidColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionClickIDWbraid
}

func (c *matomoSessionClickIDWbraidColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionClickIDWbraidColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionClickIDFbclidColumn struct{}

func (c *matomoSessionClickIDFbclidColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionClickIDFbclidColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionClickIDFbclid
}

func (c *matomoSessionClickIDFbclidColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionClickIDFbclidColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionClickIDMsclkidColumn struct{}

func (c *matomoSessionClickIDMsclkidColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionClickIDMsclkidColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionClickIDMsclkid
}

func (c *matomoSessionClickIDMsclkidColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionClickIDMsclkidColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionTotalPageViewsColumn struct{}

func (c *matomoSessionTotalPageViewsColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionTotalPageViewsColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionTotalPageViews
}

func (c *matomoSessionTotalPageViewsColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionTotalPageViewsColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUniquePageViewsColumn struct{}

func (c *matomoSessionUniquePageViewsColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUniquePageViewsColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUniquePageViews
}

func (c *matomoSessionUniquePageViewsColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUniquePageViewsColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionTotalPurchasesColumn struct{}

func (c *matomoSessionTotalPurchasesColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionTotalPurchasesColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionTotalPurchases
}

func (c *matomoSessionTotalPurchasesColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionTotalPurchasesColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionTotalScrollsColumn struct{}

func (c *matomoSessionTotalScrollsColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionTotalScrollsColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionTotalScrolls
}

func (c *matomoSessionTotalScrollsColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionTotalScrollsColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionTotalOutboundClicksColumn struct{}

func (c *matomoSessionTotalOutboundClicksColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionTotalOutboundClicksColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionTotalOutboundClicks
}

func (c *matomoSessionTotalOutboundClicksColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionTotalOutboundClicksColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUniqueOutboundClicksColumn struct{}

func (c *matomoSessionUniqueOutboundClicksColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUniqueOutboundClicksColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUniqueOutboundClicks
}

func (c *matomoSessionUniqueOutboundClicksColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUniqueOutboundClicksColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionTotalSiteSearchesColumn struct{}

func (c *matomoSessionTotalSiteSearchesColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionTotalSiteSearchesColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionTotalSiteSearches
}

func (c *matomoSessionTotalSiteSearchesColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionTotalSiteSearchesColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUniqueSiteSearchesColumn struct{}

func (c *matomoSessionUniqueSiteSearchesColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUniqueSiteSearchesColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUniqueSiteSearches
}

func (c *matomoSessionUniqueSiteSearchesColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUniqueSiteSearchesColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionTotalFormInteractionsColumn struct{}

func (c *matomoSessionTotalFormInteractionsColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionTotalFormInteractionsColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionTotalFormInteractions
}

func (c *matomoSessionTotalFormInteractionsColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionTotalFormInteractionsColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUniqueFormInteractionsColumn struct{}

func (c *matomoSessionUniqueFormInteractionsColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUniqueFormInteractionsColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUniqueFormInteractions
}

func (c *matomoSessionUniqueFormInteractionsColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUniqueFormInteractionsColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionTotalVideoEngagementsColumn struct{}

func (c *matomoSessionTotalVideoEngagementsColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionTotalVideoEngagementsColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionTotalVideoEngagements
}

func (c *matomoSessionTotalVideoEngagementsColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionTotalVideoEngagementsColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionTotalFileDownloadsColumn struct{}

func (c *matomoSessionTotalFileDownloadsColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionTotalFileDownloadsColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionTotalFileDownloads
}

func (c *matomoSessionTotalFileDownloadsColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionTotalFileDownloadsColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

type matomoSessionUniqueFileDownloadsColumn struct{}

func (c *matomoSessionUniqueFileDownloadsColumn) Docs() schema.Documentation {
	return schema.Documentation{}
}

func (c *matomoSessionUniqueFileDownloadsColumn) Implements() schema.Interface {
	return columns.CoreInterfaces.SessionUniqueFileDownloads
}

func (c *matomoSessionUniqueFileDownloadsColumn) DependsOn() []schema.DependsOnEntry {
	return nil
}

func (c *matomoSessionUniqueFileDownloadsColumn) Write(session *schema.Session) schema.D8AColumnWriteError {
	// TODO(matomo): implement
	return nil
}

var eventIgnoreReferrerColumn = &matomoEventIgnoreReferrerColumn{}
var eventDateUTCColumn = &matomoEventDateUTCColumn{}
var eventTimestampUTCColumn = &matomoEventTimestampUTCColumn{}
var eventPageReferrerColumn = &matomoEventPageReferrerColumn{}
var eventPageLocationColumn = &matomoEventPageLocationColumn{}
var eventPageHostnameColumn = &matomoEventPageHostnameColumn{}
var eventPagePathColumn = &matomoEventPagePathColumn{}
var eventPageTitleColumn = &matomoEventPageTitleColumn{}
var eventTrackingProtocolColumn = &matomoEventTrackingProtocolColumn{}
var eventPlatformColumn = &matomoEventPlatformColumn{}
var deviceLanguageColumn = &matomoDeviceLanguageColumn{}

var sseTimeOnPageColumn = &matomoSSETimeOnPageColumn{}
var sseIsEntryPageColumn = &matomoSSEIsEntryPageColumn{}
var sseIsExitPageColumn = &matomoSSEIsExitPageColumn{}

var sessionEntryPageLocationColumn = &matomoSessionEntryPageLocationColumn{}
var sessionSecondPageLocationColumn = &matomoSessionSecondPageLocationColumn{}
var sessionExitPageLocationColumn = &matomoSessionExitPageLocationColumn{}
var sessionEntryPageTitleColumn = &matomoSessionEntryPageTitleColumn{}
var sessionSecondPageTitleColumn = &matomoSessionSecondPageTitleColumn{}
var sessionExitPageTitleColumn = &matomoSessionExitPageTitleColumn{}
var sessionUtmCampaignColumn = &matomoSessionUtmCampaignColumn{}
var sessionUtmSourceColumn = &matomoSessionUtmSourceColumn{}
var sessionUtmMediumColumn = &matomoSessionUtmMediumColumn{}
var sessionUtmContentColumn = &matomoSessionUtmContentColumn{}
var sessionUtmTermColumn = &matomoSessionUtmTermColumn{}
var sessionUtmIDColumn = &matomoSessionUtmIDColumn{}
var sessionUtmSourcePlatformColumn = &matomoSessionUtmSourcePlatformColumn{}
var sessionUtmCreativeFormatColumn = &matomoSessionUtmCreativeFormatColumn{}
var sessionUtmMarketingTacticColumn = &matomoSessionUtmMarketingTacticColumn{}
var sessionClickIDGclidColumn = &matomoSessionClickIDGclidColumn{}
var sessionClickIDDclidColumn = &matomoSessionClickIDDclidColumn{}
var sessionClickIDGbraidColumn = &matomoSessionClickIDGbraidColumn{}
var sessionClickIDSrsltidColumn = &matomoSessionClickIDSrsltidColumn{}
var sessionClickIDWbraidColumn = &matomoSessionClickIDWbraidColumn{}
var sessionClickIDFbclidColumn = &matomoSessionClickIDFbclidColumn{}
var sessionClickIDMsclkidColumn = &matomoSessionClickIDMsclkidColumn{}
var sessionTotalPageViewsColumn = &matomoSessionTotalPageViewsColumn{}
var sessionUniquePageViewsColumn = &matomoSessionUniquePageViewsColumn{}
var sessionTotalPurchasesColumn = &matomoSessionTotalPurchasesColumn{}
var sessionTotalScrollsColumn = &matomoSessionTotalScrollsColumn{}
var sessionTotalOutboundClicksColumn = &matomoSessionTotalOutboundClicksColumn{}
var sessionUniqueOutboundClicksColumn = &matomoSessionUniqueOutboundClicksColumn{}
var sessionTotalSiteSearchesColumn = &matomoSessionTotalSiteSearchesColumn{}
var sessionUniqueSiteSearchesColumn = &matomoSessionUniqueSiteSearchesColumn{}
var sessionTotalFormInteractionsColumn = &matomoSessionTotalFormInteractionsColumn{}
var sessionUniqueFormInteractionsColumn = &matomoSessionUniqueFormInteractionsColumn{}
var sessionTotalVideoEngagementsColumn = &matomoSessionTotalVideoEngagementsColumn{}
var sessionTotalFileDownloadsColumn = &matomoSessionTotalFileDownloadsColumn{}
var sessionUniqueFileDownloadsColumn = &matomoSessionUniqueFileDownloadsColumn{}

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
