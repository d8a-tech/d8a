package sessioncolumns

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/protocol"
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

	return eventNameStr == protocol.PageViewEventType
}

func nthPageViewValueColumn(
	id schema.InterfaceID,
	field *arrow.Field,
	nth int,
	valueField *arrow.Field,
) schema.SessionColumn {
	return columns.NthEventMatchingPredicateValueColumn(
		id,
		field,
		nth,
		columns.ExctractFieldValue(valueField.Name),
		isPageViewEvent,
	)
}

var SessionEntryPageLocationColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionEntryPageLocation.ID,
	columns.CoreInterfaces.SessionEntryPageLocation.Field,
	0,
	columns.CoreInterfaces.EventPageLocation.Field,
)

var SessionSecondPageLocationColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionSecondPageLocation.ID,
	columns.CoreInterfaces.SessionSecondPageLocation.Field,
	1,
	columns.CoreInterfaces.EventPageLocation.Field,
)

var SessionExitPageLocationColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionExitPageLocation.ID,
	columns.CoreInterfaces.SessionExitPageLocation.Field,
	-1,
	columns.CoreInterfaces.EventPageLocation.Field,
)

var SessionEntryPageTitleColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionEntryPageTitle.ID,
	columns.CoreInterfaces.SessionEntryPageTitle.Field,
	0,
	columns.CoreInterfaces.EventPageTitle.Field,
)

var SessionSecondPageTitleColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionSecondPageTitle.ID,
	columns.CoreInterfaces.SessionSecondPageTitle.Field,
	1,
	columns.CoreInterfaces.EventPageTitle.Field,
)

var SessionExitPageTitleColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionExitPageTitle.ID,
	columns.CoreInterfaces.SessionExitPageTitle.Field,
	-1,
	columns.CoreInterfaces.EventPageTitle.Field,
)

var SessionUtmCampaignColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmCampaign.ID,
	columns.CoreInterfaces.SessionUtmCampaign.Field,
	0,
	columns.CoreInterfaces.EventUtmCampaign.Field,
)

var SessionUtmSourceColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmSource.ID,
	columns.CoreInterfaces.SessionUtmSource.Field,
	0,
	columns.CoreInterfaces.EventUtmSource.Field,
)

var SessionUtmMediumColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmMedium.ID,
	columns.CoreInterfaces.SessionUtmMedium.Field,
	0,
	columns.CoreInterfaces.EventUtmMedium.Field,
)

var SessionUtmContentColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmContent.ID,
	columns.CoreInterfaces.SessionUtmContent.Field,
	0,
	columns.CoreInterfaces.EventUtmContent.Field,
)

var SessionUtmTermColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmTerm.ID,
	columns.CoreInterfaces.SessionUtmTerm.Field,
	0,
	columns.CoreInterfaces.EventUtmTerm.Field,
)

var SessionUtmIDColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmID.ID,
	columns.CoreInterfaces.SessionUtmID.Field,
	0,
	columns.CoreInterfaces.EventUtmID.Field,
)

var SessionUtmSourcePlatformColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmSourcePlatform.ID,
	columns.CoreInterfaces.SessionUtmSourcePlatform.Field,
	0,
	columns.CoreInterfaces.EventUtmSourcePlatform.Field,
)

var SessionUtmCreativeFormatColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmCreativeFormat.ID,
	columns.CoreInterfaces.SessionUtmCreativeFormat.Field,
	0,
	columns.CoreInterfaces.EventUtmCreativeFormat.Field,
)

var SessionUtmMarketingTacticColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmMarketingTactic.ID,
	columns.CoreInterfaces.SessionUtmMarketingTactic.Field,
	0,
	columns.CoreInterfaces.EventUtmMarketingTactic.Field,
)

var SessionClickIDGclidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDGclid.ID,
	columns.CoreInterfaces.SessionClickIDGclid.Field,
	0,
	columns.CoreInterfaces.EventClickIDGclid.Field,
)

var SessionClickIDDclidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDDclid.ID,
	columns.CoreInterfaces.SessionClickIDDclid.Field,
	0,
	columns.CoreInterfaces.EventClickIDDclid.Field,
)

var SessionClickIDGbraidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDGbraid.ID,
	columns.CoreInterfaces.SessionClickIDGbraid.Field,
	0,
	columns.CoreInterfaces.EventClickIDGbraid.Field,
)

var SessionClickIDSrsltidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDSrsltid.ID,
	columns.CoreInterfaces.SessionClickIDSrsltid.Field,
	0,
	columns.CoreInterfaces.EventClickIDSrsltid.Field,
)

var SessionClickIDWbraidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDWbraid.ID,
	columns.CoreInterfaces.SessionClickIDWbraid.Field,
	0,
	columns.CoreInterfaces.EventClickIDWbraid.Field,
)

var SessionClickIDFbclidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDFbclid.ID,
	columns.CoreInterfaces.SessionClickIDFbclid.Field,
	0,
	columns.CoreInterfaces.EventClickIDFbclid.Field,
)

var SessionClickIDMsclkidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDMsclkid.ID,
	columns.CoreInterfaces.SessionClickIDMsclkid.Field,
	0,
	columns.CoreInterfaces.EventClickIDMsclkid.Field,
)

var SessionTotalPageViewsColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalPageViews.ID,
	columns.CoreInterfaces.SessionTotalPageViews.Field,
	[]string{protocol.PageViewEventType},
	columns.WithSessionColumnDocs(
		"Total Page Views",
		fmt.Sprintf(
			"The total number of page views (event name: %s) in the session.",
			protocol.PageViewEventType,
		),
	),
)

var SessionUniquePageViewsColumn = columns.UniqueEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionUniquePageViews.ID,
	columns.CoreInterfaces.SessionUniquePageViews.Field,
	[]string{protocol.PageViewEventType},
	[]*arrow.Field{columns.CoreInterfaces.EventPageLocation.Field},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageLocation.ID},
	),
	columns.WithSessionColumnDocs(
		"Unique Page Views",
		fmt.Sprintf(
			"The unique number of page views (event name: %s) in the session. Deduplicated by %s.",
			protocol.PageViewEventType,
			columns.CoreInterfaces.EventPageLocation.Field.Name,
		),
	),
)
