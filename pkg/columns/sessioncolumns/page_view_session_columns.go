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
	valueID schema.InterfaceID,
	valueField *arrow.Field,
	displayName string,
	description string,
) schema.SessionColumn {
	return columns.NthEventMatchingPredicateValueColumn(
		id,
		field,
		nth,
		columns.ExctractFieldValue(valueField.Name),
		isPageViewEvent,
		columns.WithSessionColumnDependsOn(
			schema.DependsOnEntry{Interface: valueID},
			schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
		),
		columns.WithSessionColumnDocs(displayName, description),
	)
}

var SessionEntryPageLocationColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionEntryPageLocation.ID,
	columns.CoreInterfaces.SessionEntryPageLocation.Field,
	0,
	columns.CoreInterfaces.EventPageLocation.ID,
	columns.CoreInterfaces.EventPageLocation.Field,
	"Session Entry Page Location",
	"The URL of the first page view event in the session.",
)

var SessionSecondPageLocationColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionSecondPageLocation.ID,
	columns.CoreInterfaces.SessionSecondPageLocation.Field,
	1,
	columns.CoreInterfaces.EventPageLocation.ID,
	columns.CoreInterfaces.EventPageLocation.Field,
	"Session Second Page Location",
	"The URL of the second page view event in the session. "+
		"Useful for analyzing user navigation patterns after landing.",
)

var SessionExitPageLocationColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionExitPageLocation.ID,
	columns.CoreInterfaces.SessionExitPageLocation.Field,
	-1,
	columns.CoreInterfaces.EventPageLocation.ID,
	columns.CoreInterfaces.EventPageLocation.Field,
	"Session Exit Page Location",
	"The URL of the last page view event in the session.",
)

var SessionEntryPageTitleColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionEntryPageTitle.ID,
	columns.CoreInterfaces.SessionEntryPageTitle.Field,
	0,
	columns.CoreInterfaces.EventPageTitle.ID,
	columns.CoreInterfaces.EventPageTitle.Field,
	"Session Entry Page Title",
	"The title of the first page view event in the session.",
)

var SessionSecondPageTitleColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionSecondPageTitle.ID,
	columns.CoreInterfaces.SessionSecondPageTitle.Field,
	1,
	columns.CoreInterfaces.EventPageTitle.ID,
	columns.CoreInterfaces.EventPageTitle.Field,
	"Session Second Page Title",
	"The title of the second page view event in the session. "+
		"Useful for analyzing user navigation patterns after landing.",
)

var SessionExitPageTitleColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionExitPageTitle.ID,
	columns.CoreInterfaces.SessionExitPageTitle.Field,
	-1,
	columns.CoreInterfaces.EventPageTitle.ID,
	columns.CoreInterfaces.EventPageTitle.Field,
	"Session Exit Page Title",
	"The title of the last page view event in the session.",
)

var SessionUtmCampaignColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmCampaign.ID,
	columns.CoreInterfaces.SessionUtmCampaign.Field,
	0,
	columns.CoreInterfaces.EventUtmCampaign.ID,
	columns.CoreInterfaces.EventUtmCampaign.Field,
	"Session UTM Campaign",
	"The UTM campaign from the first page view event in the session.",
)

var SessionUtmSourceColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmSource.ID,
	columns.CoreInterfaces.SessionUtmSource.Field,
	0,
	columns.CoreInterfaces.EventUtmSource.ID,
	columns.CoreInterfaces.EventUtmSource.Field,
	"Session UTM Source",
	"The UTM source from the first page view event in the session.",
)

var SessionUtmMediumColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmMedium.ID,
	columns.CoreInterfaces.SessionUtmMedium.Field,
	0,
	columns.CoreInterfaces.EventUtmMedium.ID,
	columns.CoreInterfaces.EventUtmMedium.Field,
	"Session UTM Medium",
	"The UTM medium from the first page view event in the session.",
)

var SessionUtmContentColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmContent.ID,
	columns.CoreInterfaces.SessionUtmContent.Field,
	0,
	columns.CoreInterfaces.EventUtmContent.ID,
	columns.CoreInterfaces.EventUtmContent.Field,
	"Session UTM Content",
	"The UTM content from the first page view event in the session.",
)

var SessionUtmTermColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmTerm.ID,
	columns.CoreInterfaces.SessionUtmTerm.Field,
	0,
	columns.CoreInterfaces.EventUtmTerm.ID,
	columns.CoreInterfaces.EventUtmTerm.Field,
	"Session UTM Term",
	"The UTM term from the first page view event in the session.",
)

var SessionUtmIDColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmID.ID,
	columns.CoreInterfaces.SessionUtmID.Field,
	0,
	columns.CoreInterfaces.EventUtmID.ID,
	columns.CoreInterfaces.EventUtmID.Field,
	"Session UTM ID",
	"The UTM ID from the first page view event in the session.",
)

var SessionUtmSourcePlatformColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmSourcePlatform.ID,
	columns.CoreInterfaces.SessionUtmSourcePlatform.Field,
	0,
	columns.CoreInterfaces.EventUtmSourcePlatform.ID,
	columns.CoreInterfaces.EventUtmSourcePlatform.Field,
	"Session UTM Source Platform",
	"The UTM source platform from the first page view event in the session.",
)

var SessionUtmCreativeFormatColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmCreativeFormat.ID,
	columns.CoreInterfaces.SessionUtmCreativeFormat.Field,
	0,
	columns.CoreInterfaces.EventUtmCreativeFormat.ID,
	columns.CoreInterfaces.EventUtmCreativeFormat.Field,
	"Session UTM Creative Format",
	"The UTM creative format from the first page view event in the session.",
)

var SessionUtmMarketingTacticColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionUtmMarketingTactic.ID,
	columns.CoreInterfaces.SessionUtmMarketingTactic.Field,
	0,
	columns.CoreInterfaces.EventUtmMarketingTactic.ID,
	columns.CoreInterfaces.EventUtmMarketingTactic.Field,
	"Session UTM Marketing Tactic",
	"The UTM marketing tactic from the first page view event in the session.",
)

var SessionClickIDGclidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDGclid.ID,
	columns.CoreInterfaces.SessionClickIDGclid.Field,
	0,
	columns.CoreInterfaces.EventClickIDGclid.ID,
	columns.CoreInterfaces.EventClickIDGclid.Field,
	"Session Click ID GCLID",
	"The Google Click ID (gclid) from the first page view event in the session.",
)

var SessionClickIDDclidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDDclid.ID,
	columns.CoreInterfaces.SessionClickIDDclid.Field,
	0,
	columns.CoreInterfaces.EventClickIDDclid.ID,
	columns.CoreInterfaces.EventClickIDDclid.Field,
	"Session Click ID DCLID",
	"The Google Display & Video 360 Click ID (dclid) "+
		"from the first page view event in the session.",
)

var SessionClickIDGbraidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDGbraid.ID,
	columns.CoreInterfaces.SessionClickIDGbraid.Field,
	0,
	columns.CoreInterfaces.EventClickIDGbraid.ID,
	columns.CoreInterfaces.EventClickIDGbraid.Field,
	"Session Click ID GBRAID",
	"The Google Click ID for iOS app-to-web conversions (gbraid) "+
		"from the first page view event in the session.",
)

var SessionClickIDSrsltidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDSrsltid.ID,
	columns.CoreInterfaces.SessionClickIDSrsltid.Field,
	0,
	columns.CoreInterfaces.EventClickIDSrsltid.ID,
	columns.CoreInterfaces.EventClickIDSrsltid.Field,
	"Session Click ID SRSLTID",
	"The Google Shopping Result Click ID (srsltid) from the first page view event in the session.",
)

var SessionClickIDWbraidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDWbraid.ID,
	columns.CoreInterfaces.SessionClickIDWbraid.Field,
	0,
	columns.CoreInterfaces.EventClickIDWbraid.ID,
	columns.CoreInterfaces.EventClickIDWbraid.Field,
	"Session Click ID WBRAID",
	"The Google Click ID for iOS web-to-app conversions (wbraid) "+
		"from the first page view event in the session.",
)

var SessionClickIDFbclidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDFbclid.ID,
	columns.CoreInterfaces.SessionClickIDFbclid.Field,
	0,
	columns.CoreInterfaces.EventClickIDFbclid.ID,
	columns.CoreInterfaces.EventClickIDFbclid.Field,
	"Session Click ID FBCLID",
	"The Meta Click ID (fbclid) from the first page view event in the session.",
)

var SessionClickIDMsclkidColumn = nthPageViewValueColumn(
	columns.CoreInterfaces.SessionClickIDMsclkid.ID,
	columns.CoreInterfaces.SessionClickIDMsclkid.Field,
	0,
	columns.CoreInterfaces.EventClickIDMsclkid.ID,
	columns.CoreInterfaces.EventClickIDMsclkid.Field,
	"Session Click ID MSCLKID",
	"The Microsoft Click ID (msclkid) from the first page view event in the session.",
)

var SessionTotalPageViewsColumn = columns.TotalEventsOfGivenNameColumn(
	columns.CoreInterfaces.SessionTotalPageViews.ID,
	columns.CoreInterfaces.SessionTotalPageViews.Field,
	[]string{protocol.PageViewEventType},
	columns.WithSessionColumnDocs(
		"Session Total Page Views",
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
		"Session Unique Page Views",
		fmt.Sprintf(
			"The unique number of page views (event name: %s) in the session. Deduplicated by %s.",
			protocol.PageViewEventType,
			columns.CoreInterfaces.EventPageLocation.Field.Name,
		),
	),
)

var SessionIsBouncedColumn = columns.NewSimpleSessionColumn(
	columns.CoreInterfaces.SessionIsBounced.ID,
	columns.CoreInterfaces.SessionIsBounced.Field,
	func(session *schema.Session) (any, schema.D8AColumnWriteError) {
		pageViewCount := 0
		for _, event := range session.Events {
			if isPageViewEvent(event) {
				pageViewCount++
			}
		}

		return pageViewCount == 1, nil
	},
	columns.WithSessionColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionColumnDocs(
		"Session Is Bounced",
		"A boolean flag indicating whether the session has exactly one page view event.",
	),
)
