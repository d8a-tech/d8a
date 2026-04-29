package ga4

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
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
			Interface: ProtocolInterfaces.EventParamLinkURL.ID,
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
			Interface: ProtocolInterfaces.EventParamFormID.ID,
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
			Interface: ProtocolInterfaces.EventParamLinkURL.ID,
		},
	),
	columns.WithSessionColumnDocs(
		"Session Unique File Downloads",
		fmt.Sprintf("The unique number of file downloads (event name: %s) in the session. Deduplicated by %s.", FileDownloadEventType, ProtocolInterfaces.EventParamLinkURL.Field.Name), // nolint:lll // it's a description
	),
)
