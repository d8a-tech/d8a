package matomo

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// sessionUniqueOutboundClicksColumn counts distinct outbound link URLs clicked in the session.
var sessionUniqueOutboundClicksColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniqueOutboundClicks.ID,
	columns.CoreInterfaces.SessionUniqueOutboundClicks.Field,
	[]string{outlinkEventType},
	[]*arrow.Field{
		ProtocolInterfaces.EventLinkURL.Field,
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface: ProtocolInterfaces.EventLinkURL.ID,
		},
	),
	columns.WithSessionColumnDocs(
		"Unique Outbound Clicks",
		fmt.Sprintf("The unique number of outbound link clicks (event name: %s) in the session. Deduplicated by %s.", outlinkEventType, ProtocolInterfaces.EventLinkURL.Field.Name), //nolint:lll // description
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
		ProtocolInterfaces.EventSearchTerm.Field,
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface: ProtocolInterfaces.EventSearchTerm.ID,
		},
	),
	columns.WithSessionColumnDocs(
		"Unique Site Searches",
		fmt.Sprintf("The unique number of site searches (event name: %s) in the session. Deduplicated by %s.", siteSearchEventType, ProtocolInterfaces.EventSearchTerm.Field.Name), //nolint:lll // description
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
		"Not supported in the Matomo protocol. Form interaction tracking has no standard query-parameter mapping and is always null.",
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
		"Not supported in the Matomo protocol. Form interaction tracking has no standard query-parameter mapping and is always null.",
	),
)
