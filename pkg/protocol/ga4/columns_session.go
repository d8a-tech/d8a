package ga4

import (
	"fmt"
	"slices"

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
	columns.CoreInterfaces.EventPageLocation.Field.Name,
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
	columns.CoreInterfaces.EventPageLocation.Field.Name,
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
	columns.CoreInterfaces.EventPageTitle.Field.Name,
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
	columns.CoreInterfaces.EventPageTitle.Field.Name,
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
	columns.CoreInterfaces.EventPageLocation.Field.Name,
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
	columns.CoreInterfaces.EventPageTitle.Field.Name,
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
	columns.CoreInterfaces.EventUtmMarketingTactic.Field.Name,
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
	columns.CoreInterfaces.EventUtmSourcePlatform.Field.Name,
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
	columns.CoreInterfaces.EventUtmTerm.Field.Name,
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
	columns.CoreInterfaces.EventUtmContent.Field.Name,
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
	columns.CoreInterfaces.EventUtmSource.Field.Name,
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
	columns.CoreInterfaces.EventUtmMedium.Field.Name,
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
	columns.CoreInterfaces.EventUtmCampaign.Field.Name,
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
	columns.CoreInterfaces.EventUtmID.Field.Name,
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
	columns.CoreInterfaces.EventUtmCreativeFormat.Field.Name,
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

func TotalEventsOfGivenNameColumn(
	columnID schema.InterfaceID,
	field *arrow.Field,
	eventNames []string,
	options ...columns.SessionColumnOptions,
) schema.SessionColumn {
	options = append(options, columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	))
	return columns.NewSimpleSessionColumn(
		columnID,
		field,
		func(session *schema.Session) (any, error) {
			totalEvents := 0
			for _, event := range session.Events {
				valueAsString, ok := event.Values[columns.CoreInterfaces.EventName.Field.Name].(string)
				if !ok {
					continue
				}
				if slices.Contains(eventNames, valueAsString) {
					totalEvents++
				}
			}
			return totalEvents, nil
		},
		options...,
	)
}

func UniqueEventsOfGivenNameColumn(
	columnID schema.InterfaceID,
	field *arrow.Field,
	eventNames []string,
	dependentColumns []*arrow.Field,
	options ...columns.SessionColumnOptions,
) schema.SessionColumn {
	options = append(options, columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{
			Interface:        columns.CoreInterfaces.EventName.ID,
			GreaterOrEqualTo: "1.0.0",
		},
	))
	return columns.NewSimpleSessionColumn(
		columnID,
		field,
		func(session *schema.Session) (any, error) {
			uniqueEvents := make(map[string]bool)
			for _, event := range session.Events {
				valueAsString, ok := event.Values[columns.CoreInterfaces.EventName.Field.Name].(string)
				if !ok {
					continue
				}
				if slices.Contains(eventNames, valueAsString) {
					depHash := ""
					for _, dependentColumn := range dependentColumns {
						depValue, ok := event.Values[dependentColumn.Name]
						if !ok {
							continue
						}
						depHash += fmt.Sprintf("%v", depValue)
					}
					uniqueEvents[depHash] = true
				}
			}
			return len(uniqueEvents), nil
		},
		options...,
	)
}

var sessionTotalPageViewsColumn = TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalPageViews.ID,
	columns.CoreInterfaces.SessionTotalPageViews.Field,
	[]string{PageViewEventType},
)

var sessionUniquePageViewsColumn = UniqueEventsOfGivenNameColumn(
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
)

var sessionTotalScrollsColumn = TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalScrolls.ID,
	columns.CoreInterfaces.SessionTotalScrolls.Field,
	[]string{ScrollEventType},
)

var sessionTotalOutboundClicksColumn = TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalOutboundClicks.ID,
	columns.CoreInterfaces.SessionTotalOutboundClicks.Field,
	[]string{ClickEventType},
)

var sessionUniqueOutboundClicksColumn = UniqueEventsOfGivenNameColumn(
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
)

var sessionTotalSiteSearchesColumn = TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalSiteSearches.ID,
	columns.CoreInterfaces.SessionTotalSiteSearches.Field,
	[]string{ViewSearchResultsEventType, SearchEventType},
)

var sessionUniqueSiteSearchesColumn = UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniqueSiteSearches.ID,
	columns.CoreInterfaces.SessionUniqueSiteSearches.Field,
	[]string{ViewSearchResultsEventType, SearchEventType},
	[]*arrow.Field{
		ProtocolInterfaces.EventParamSearchTerm.Field,
	},
)

/*

	SessionTotalSiteSearches      schema.Interface
	SessionUniqueSiteSearches     schema.Interface

	SessionTotalFormInteractions  schema.Interface
	SessionUniqueFormInteractions schema.Interface

	SessionTotalVideoEngagements  schema.Interface

	SessionTotalFileDownloads     schema.Interface
	SessionUniqueFileDownloads    schema.Interface*/
