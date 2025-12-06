package ga4

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
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
	return eventNameStr == PageViewEventType
}

var sessionEntryPageLocationColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionEntryPageLocation.ID,
	columns.CoreInterfaces.SessionEntryPageLocation.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageLocation.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Entry Page Location",
		"The URL of the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionExitPageLocationColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionExitPageLocation.ID,
	columns.CoreInterfaces.SessionExitPageLocation.Field,
	-1,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageLocation.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Exit Page Location",
		"The URL of the last page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionEntryPageTitleColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionEntryPageTitle.ID,
	columns.CoreInterfaces.SessionEntryPageTitle.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageTitle.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Entry Page Title",
		"The title of the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionExitPageTitleColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionExitPageTitle.ID,
	columns.CoreInterfaces.SessionExitPageTitle.Field,
	-1,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageTitle.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Exit Page Title",
		"The title of the last page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionSecondPageLocationColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionSecondPageLocation.ID,
	columns.CoreInterfaces.SessionSecondPageLocation.Field,
	1,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageLocation.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Second Page Location",
		"The URL of the second page view event in the session. Useful for analyzing user navigation patterns after landing.", // nolint:lll // it's a description
	),
)

var sessionSecondPageTitleColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionSecondPageTitle.ID,
	columns.CoreInterfaces.SessionSecondPageTitle.Field,
	1,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventPageTitle.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageTitle.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Second Page Title",
		"The title of the second page view event in the session. Useful for analyzing user navigation patterns after landing.", // nolint:lll // it's a description
	),
)

var sessionUtmMarketingTacticColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmMarketingTactic.ID,
	columns.CoreInterfaces.SessionUtmMarketingTactic.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmMarketingTactic.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmMarketingTactic.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Marketing Tactic",
		"The UTM marketing tactic from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionUtmSourcePlatformColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmSourcePlatform.ID,
	columns.CoreInterfaces.SessionUtmSourcePlatform.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmSourcePlatform.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmSourcePlatform.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Source Platform",
		"The UTM source platform from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionUtmTermColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmTerm.ID,
	columns.CoreInterfaces.SessionUtmTerm.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmTerm.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmTerm.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Term",
		"The UTM term from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionUtmContentColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmContent.ID,
	columns.CoreInterfaces.SessionUtmContent.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmContent.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmContent.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Content",
		"The UTM content from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionUtmSourceColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmSource.ID,
	columns.CoreInterfaces.SessionUtmSource.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmSource.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmSource.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Source",
		"The UTM source from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionUtmMediumColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmMedium.ID,
	columns.CoreInterfaces.SessionUtmMedium.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmMedium.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmMedium.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Medium",
		"The UTM medium from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionUtmCampaignColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmCampaign.ID,
	columns.CoreInterfaces.SessionUtmCampaign.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmCampaign.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmCampaign.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Campaign",
		"The UTM campaign from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionUtmIDColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmID.ID,
	columns.CoreInterfaces.SessionUtmID.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmID.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmID.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session UTM ID",
		"The UTM ID from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionUtmCreativeFormatColumn = columns.NthEventMatchingPredicateValueColumn(
	columns.CoreInterfaces.SessionUtmCreativeFormat.ID,
	columns.CoreInterfaces.SessionUtmCreativeFormat.Field,
	0,
	columns.ExctractFieldValue(columns.CoreInterfaces.EventUtmCreativeFormat.Field.Name),
	isPageViewEvent,
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventUtmCreativeFormat.ID,
			GreaterOrEqualTo: "1.0.0",
		},
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session UTM Creative Format",
		"The UTM creative format from the first page view event in the session.", // nolint:lll // it's a description
	),
)

var sessionTotalPageViewsColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalPageViews.ID,
	columns.CoreInterfaces.SessionTotalPageViews.Field,
	[]string{PageViewEventType},
	columns.WithSessionColumnDocs(
		"Session Total Page Views",
		fmt.Sprintf("The total number of page views (event name: %s) in the session.", PageViewEventType), // nolint:lll // it's a description
	),
)

var sessionUniquePageViewsColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniquePageViews.ID,
	columns.CoreInterfaces.SessionUniquePageViews.Field,
	[]string{PageViewEventType},
	[]*arrow.Field{
		columns.CoreInterfaces.EventPageLocation.Field,
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventPageLocation.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Unique Page Views",
		fmt.Sprintf("The unique number of page views (event name: %s) in the session. Deduplicated by %s.", PageViewEventType, columns.CoreInterfaces.EventPageLocation.Field.Name), // nolint:lll // it's a description
	),
)

var sessionTotalPurchasesColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalPurchases.ID,
	columns.CoreInterfaces.SessionTotalPurchases.Field,
	[]string{PurchaseEventType},
	columns.WithSessionColumnDocs(
		"Session Total Purchases",
		fmt.Sprintf("The total number of purchases (event name: %s) in the session.", PurchaseEventType), // nolint:lll // it's a description
	),
)

var sessionTotalScrollsColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalScrolls.ID,
	columns.CoreInterfaces.SessionTotalScrolls.Field,
	[]string{ScrollEventType},
	columns.WithSessionColumnDocs(
		"Session Total Scrolls",
		fmt.Sprintf("The total number of scrolls (event name: %s) in the session.", ScrollEventType), // nolint:lll // it's a description
	),
)

var sessionTotalOutboundClicksColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalOutboundClicks.ID,
	columns.CoreInterfaces.SessionTotalOutboundClicks.Field,
	[]string{ClickEventType},
	columns.WithSessionColumnDocs(
		"Session Total Outbound Clicks",
		fmt.Sprintf("The total number of outbound clicks (event name: %s) in the session.", ClickEventType), // nolint:lll // it's a description
	),
)

var sessionUniqueOutboundClicksColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniqueOutboundClicks.ID,
	columns.CoreInterfaces.SessionUniqueOutboundClicks.Field,
	[]string{ClickEventType},
	[]*arrow.Field{
		ProtocolInterfaces.EventParamLinkURL.Field,
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventParamLinkURL.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Unique Outbound Clicks",
		fmt.Sprintf("The unique number of outbound clicks (event name: %s) in the session. Deduplicated by %s.", ClickEventType, ProtocolInterfaces.EventParamLinkURL.Field.Name), // nolint:lll // it's a description
	),
)

var sessionTotalSiteSearchesColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalSiteSearches.ID,
	columns.CoreInterfaces.SessionTotalSiteSearches.Field,
	[]string{ViewSearchResultsEventType, SearchEventType},
	columns.WithSessionColumnDocs(
		"Session Total Site Searches",
		fmt.Sprintf("The total number of site searches (event name: %s or %s) in the session.", ViewSearchResultsEventType, SearchEventType), // nolint:lll // it's a description
	),
)

var sessionUniqueSiteSearchesColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniqueSiteSearches.ID,
	columns.CoreInterfaces.SessionUniqueSiteSearches.Field,
	[]string{ViewSearchResultsEventType, SearchEventType},
	[]*arrow.Field{
		ProtocolInterfaces.EventParamSearchTerm.Field,
	},
	columns.WithSessionColumnDocs(
		"Session Unique Site Searches",
		fmt.Sprintf("The unique number of site searches (event name: %s or %s) in the session. Deduplicated by %s.", ViewSearchResultsEventType, SearchEventType, ProtocolInterfaces.EventParamSearchTerm.Field.Name), // nolint:lll // it's a description
	),
)

var sessionTotalFormInteractionsColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalFormInteractions.ID,
	columns.CoreInterfaces.SessionTotalFormInteractions.Field,
	[]string{FormSubmitEventType, FormStartEventType},
	columns.WithSessionColumnDocs(
		"Session Total Form Interactions",
		fmt.Sprintf("The total number of form interactions (event name: %s or %s) in the session.", FormSubmitEventType, FormStartEventType), // nolint:lll // it's a description
	),
)

var sessionUniqueFormInteractionsColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniqueFormInteractions.ID,
	columns.CoreInterfaces.SessionUniqueFormInteractions.Field,
	[]string{FormSubmitEventType, FormStartEventType},
	[]*arrow.Field{
		ProtocolInterfaces.EventParamFormID.Field,
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventParamFormID.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Unique Form Interactions",
		fmt.Sprintf("The unique number of form interactions (event name: %s or %s) in the session. Deduplicated by %s.", FormSubmitEventType, FormStartEventType, ProtocolInterfaces.EventParamFormID.Field.Name), // nolint:lll // it's a description
	),
)

var sessionTotalVideoEngagementsColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalVideoEngagements.ID,
	columns.CoreInterfaces.SessionTotalVideoEngagements.Field,
	[]string{VideoStartEventType, VideoCompleteEventType, VideoProgressEventType},
	columns.WithSessionColumnDocs(
		"Session Total Video Engagements",
		fmt.Sprintf("The total number of video engagements (event name: %s, %s or %s) in the session.", VideoStartEventType, VideoCompleteEventType, VideoProgressEventType), // nolint:lll // it's a description
	),
)

var sessionTotalFileDownloadsColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalFileDownloads.ID,
	columns.CoreInterfaces.SessionTotalFileDownloads.Field,
	[]string{FileDownloadEventType},
	columns.WithSessionColumnDocs(
		"Session Total File Downloads",
		fmt.Sprintf("The total number of file downloads (event name: %s) in the session.", FileDownloadEventType), // nolint:lll // it's a description
	),
)

var sessionUniqueFileDownloadsColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniqueFileDownloads.ID,
	columns.CoreInterfaces.SessionUniqueFileDownloads.Field,
	[]string{FileDownloadEventType},
	[]*arrow.Field{
		ProtocolInterfaces.EventParamLinkURL.Field,
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        ProtocolInterfaces.EventParamLinkURL.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	),
	columns.WithSessionColumnDocs(
		"Session Unique File Downloads",
		fmt.Sprintf("The unique number of file downloads (event name: %s) in the session. Deduplicated by %s.", FileDownloadEventType, ProtocolInterfaces.EventParamLinkURL.Field.Name), // nolint:lll // it's a description
	),
)

var sessionSourceColumn = columns.SessionSourceColumn(
	isPageViewEvent,
)

var sessionMediumColumn = columns.SessionMediumColumn(
	isPageViewEvent,
)

var sessionTermColumn = columns.SessionTermColumn(
	isPageViewEvent,
)
