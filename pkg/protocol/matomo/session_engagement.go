//nolint:godox,dupl,nilnil // TODO comments are intentional stubs.
package matomo

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// sessionTotalPurchasesColumn counts completed ecommerce orders in the session.
var sessionTotalPurchasesColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalPurchases.ID,
	columns.CoreInterfaces.SessionTotalPurchases.Field,
	[]string{ecOrderEventType},
	columns.WithSessionColumnDocs(
		"Total Purchases",
		fmt.Sprintf("The total number of completed ecommerce orders (event name: %s) in the session. Detected via idgoal=0 with a non-empty ec_id parameter.", ecOrderEventType), //nolint:lll // description
	),
)

// sessionTotalGoalConversionsColumn counts goal conversion events in the session.
var sessionTotalGoalConversionsColumn = columns.TotalEventsOfGivenNameColumn(
	ProtocolInterfaces.SessionTotalGoalConversions.ID,
	ProtocolInterfaces.SessionTotalGoalConversions.Field,
	[]string{goalConversionEventType},
	columns.WithSessionColumnDocs(
		"Total Goal Conversions",
		fmt.Sprintf("The total number of goal conversions (event name: %s) in the session.", goalConversionEventType), //nolint:lll // description
	),
)

// sessionTotalScrollsColumn is not supported in the Matomo protocol and always returns null.
var sessionTotalScrollsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalScrolls.ID,
	columns.CoreInterfaces.SessionTotalScrolls.Field,
	func(_ *schema.Session) (any, schema.D8AColumnWriteError) {
		return nil, nil //nolint:nilnil // not supported in Matomo
	},
	columns.WithSessionColumnDocs(
		"Total Scrolls",
		"Not supported in the Matomo protocol. "+
			"Scroll depth tracking has no standard query-parameter mapping and is always null.",
	),
)

// sessionTotalOutboundClicksColumn counts outbound link click events in the session.
var sessionTotalOutboundClicksColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalOutboundClicks.ID,
	columns.CoreInterfaces.SessionTotalOutboundClicks.Field,
	[]string{outlinkEventType},
	columns.WithSessionColumnDocs(
		"Total Outbound Clicks",
		fmt.Sprintf("The total number of outbound link clicks (event name: %s) in the session.", outlinkEventType), //nolint:lll // description
	),
)

// sessionUniqueOutboundClicksColumn counts distinct outbound link URLs clicked in the session.
var sessionUniqueOutboundClicksColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniqueOutboundClicks.ID,
	columns.CoreInterfaces.SessionUniqueOutboundClicks.Field,
	[]string{outlinkEventType},
	[]*arrow.Field{
		ProtocolInterfaces.EventParamsLinkURL.Field,
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface: ProtocolInterfaces.EventParamsLinkURL.ID,
		},
	),
	columns.WithSessionColumnDocs(
		"Unique Outbound Clicks",
		fmt.Sprintf("The unique number of outbound link clicks (event name: %s) in the session. Deduplicated by %s.", outlinkEventType, ProtocolInterfaces.EventParamsLinkURL.Field.Name), //nolint:lll // description
	),
)

// sessionTotalSiteSearchesColumn counts all site search events in the session.
var sessionTotalSiteSearchesColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalSiteSearches.ID,
	columns.CoreInterfaces.SessionTotalSiteSearches.Field,
	[]string{siteSearchEventType},
	columns.WithSessionColumnDocs(
		"Total Site Searches",
		fmt.Sprintf("The total number of site searches (event name: %s) in the session.", siteSearchEventType), //nolint:lll // description
	),
)

// sessionUniqueSiteSearchesColumn counts distinct search terms used in the session.
var sessionUniqueSiteSearchesColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniqueSiteSearches.ID,
	columns.CoreInterfaces.SessionUniqueSiteSearches.Field,
	[]string{siteSearchEventType},
	[]*arrow.Field{
		ProtocolInterfaces.EventParamsSearchTerm.Field,
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface: ProtocolInterfaces.EventParamsSearchTerm.ID,
		},
	),
	columns.WithSessionColumnDocs(
		"Unique Site Searches",
		fmt.Sprintf("The unique number of site searches (event name: %s) in the session. Deduplicated by %s.", siteSearchEventType, ProtocolInterfaces.EventParamsSearchTerm.Field.Name), //nolint:lll // description
	),
)

// sessionTotalFormInteractionsColumn is not supported in the Matomo protocol and always returns null.
var sessionTotalFormInteractionsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionTotalFormInteractions.ID,
	columns.CoreInterfaces.SessionTotalFormInteractions.Field,
	func(_ *schema.Session) (any, schema.D8AColumnWriteError) {
		return nil, nil //nolint:nilnil // not supported in Matomo
	},
	columns.WithSessionColumnDocs(
		"Total Form Interactions",
		"Not supported in the Matomo protocol. "+
			"Form interaction tracking has no standard query-parameter mapping and is always null.",
	),
)

// sessionUniqueFormInteractionsColumn is not supported in the Matomo protocol and always returns null.
var sessionUniqueFormInteractionsColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionUniqueFormInteractions.ID,
	columns.CoreInterfaces.SessionUniqueFormInteractions.Field,
	func(_ *schema.Session) (any, schema.D8AColumnWriteError) {
		return nil, nil //nolint:nilnil // not supported in Matomo
	},
	columns.WithSessionColumnDocs(
		"Unique Form Interactions",
		"Not supported in the Matomo protocol. "+
			"Form interaction tracking has no standard query-parameter mapping and is always null.",
	),
)

// sessionTotalVideoEngagementsColumn counts all video play events in the session.
var sessionTotalVideoEngagementsColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalVideoEngagements.ID,
	columns.CoreInterfaces.SessionTotalVideoEngagements.Field,
	[]string{videoPlayEventType},
	columns.WithSessionColumnDocs(
		"Total Video Engagements",
		fmt.Sprintf("The total number of video play events (event name: %s) in the session.", videoPlayEventType), //nolint:lll // description
	),
)

// sessionTotalFileDownloadsColumn counts all file download events in the session.
var sessionTotalFileDownloadsColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalFileDownloads.ID,
	columns.CoreInterfaces.SessionTotalFileDownloads.Field,
	[]string{downloadEventType},
	columns.WithSessionColumnDocs(
		"Total File Downloads",
		fmt.Sprintf("The total number of file download events (event name: %s) in the session.", downloadEventType), //nolint:lll // description
	),
)

// sessionUniqueFileDownloadsColumn counts distinct downloaded file URLs in the session.
var sessionUniqueFileDownloadsColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniqueFileDownloads.ID,
	columns.CoreInterfaces.SessionUniqueFileDownloads.Field,
	[]string{downloadEventType},
	[]*arrow.Field{
		ProtocolInterfaces.EventParamsDownloadURL.Field,
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface: ProtocolInterfaces.EventParamsDownloadURL.ID,
		},
	),
	columns.WithSessionColumnDocs(
		"Unique File Downloads",
		fmt.Sprintf("The unique number of file downloads (event name: %s) in the session. Deduplicated by %s.", downloadEventType, ProtocolInterfaces.EventParamsDownloadURL.Field.Name), //nolint:lll // description
	),
)
