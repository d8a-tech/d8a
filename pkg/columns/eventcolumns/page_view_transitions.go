package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/protocol"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var sseTransitionOnPageView = columns.TransitionAdvanceWhenEventNameIs(protocol.PageViewEventType)

var SSETimeOnPage = columns.NewTimeOnPageColumn(
	columns.CoreInterfaces.SSETimeOnPage.ID,
	columns.CoreInterfaces.SSETimeOnPage.Field,
	sseTransitionOnPageView,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Time On Page",
		"Time spent on a particular page, calculated as the interval between subsequent page view events in seconds, or using other events timestamps if no subsequent page view was recorded.", // nolint:lll // it's a description
	),
)

var SSEIsEntryPage = columns.NewFirstLastMatchingEventColumn(
	columns.CoreInterfaces.SSEIsEntryPage.ID,
	columns.CoreInterfaces.SSEIsEntryPage.Field,
	sseTransitionOnPageView,
	true,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Session Is Entry Page",
		"A boolean flag indicating whether this event is the first page view in the session. Returns true for the first page view event in the session, false for all other events. Returns false if there are no page views in the session.", // nolint:lll // it's a description
	),
)

var SSEIsExitPage = columns.NewFirstLastMatchingEventColumn(
	columns.CoreInterfaces.SSEIsExitPage.ID,
	columns.CoreInterfaces.SSEIsExitPage.Field,
	sseTransitionOnPageView,
	false,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Session Is Exit Page",
		"A boolean flag indicating whether this event is the last page view in the session. Returns true for the last page view event in the session, false for all other events. Returns false if there are no page views in the session.", // nolint:lll // it's a description
	),
)

var SSEIsBounce = columns.NewSimpleSessionScopedEventColumn(
	columns.CoreInterfaces.SSEIsBounce.ID,
	columns.CoreInterfaces.SSEIsBounce.Field,
	func(s *schema.Session, i int) (any, schema.D8AColumnWriteError) {
		pageViewCount := 0
		pageViewIndex := -1

		for idx, event := range s.Events {
			if !sseTransitionOnPageView(event) {
				continue
			}

			pageViewCount++
			pageViewIndex = idx
			if pageViewCount > 1 {
				return false, nil
			}
		}

		return pageViewCount == 1 && i == pageViewIndex, nil
	},
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventName.ID},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Is Bounce",
		"A boolean flag indicating whether this event is the single page view of a bounced session. Returns true only for that page view event and false for all other events.", // nolint:lll // it's a description
	),
)

var EventPreviousPageLocation = columns.NewValueTransitionColumn(
	columns.CoreInterfaces.EventPreviousPageLocation.ID,
	columns.CoreInterfaces.EventPreviousPageLocation.Field,
	columns.CoreInterfaces.EventPageLocation.Field.Name,
	sseTransitionOnPageView,
	columns.TransitionDirectionBackward,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageLocation.ID},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Previous Page Location",
		"The URL of the previous page viewed in the session before the current page. Only populated when a page transition is detected. Returns nil for the first page or when no page change has occurred.", // nolint:lll // it's a description
	),
)

var EventNextPageLocation = columns.NewValueTransitionColumn(
	columns.CoreInterfaces.EventNextPageLocation.ID,
	columns.CoreInterfaces.EventNextPageLocation.Field,
	columns.CoreInterfaces.EventPageLocation.Field.Name,
	sseTransitionOnPageView,
	columns.TransitionDirectionForward,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageLocation.ID},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Next Page Location",
		"The URL of the next page viewed in the session after the current page. Only populated when a page transition is detected. Returns nil for the first page or when no page change has occurred.", // nolint:lll // it's a description
	),
)

var EventPreviousPageTitle = columns.NewValueTransitionColumn(
	columns.CoreInterfaces.EventPreviousPageTitle.ID,
	columns.CoreInterfaces.EventPreviousPageTitle.Field,
	columns.CoreInterfaces.EventPageTitle.Field.Name,
	sseTransitionOnPageView,
	columns.TransitionDirectionBackward,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageTitle.ID},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Previous Page Title",
		"The title of the previous page viewed in the session before the current page. Only populated when a page transition is detected. Returns nil for the first page or when no page change has occurred.", // nolint:lll // it's a description
	),
)

var EventNextPageTitle = columns.NewValueTransitionColumn(
	columns.CoreInterfaces.EventNextPageTitle.ID,
	columns.CoreInterfaces.EventNextPageTitle.Field,
	columns.CoreInterfaces.EventPageTitle.Field.Name,
	sseTransitionOnPageView,
	columns.TransitionDirectionForward,
	columns.WithSessionScopedEventColumnRequired(false),
	columns.WithSessionScopedEventColumnDependsOn(
		schema.DependsOnEntry{Interface: columns.CoreInterfaces.EventPageTitle.ID},
	),
	columns.WithSessionScopedEventColumnDocs(
		"Next Page Title",
		"The title of the next page viewed in the session after the current page. Only populated when a page transition is detected. Returns nil for the first page or when no page change has occurred.", // nolint:lll // it's a description
	),
)
