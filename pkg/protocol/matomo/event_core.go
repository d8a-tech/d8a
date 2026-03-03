package matomo

import (
	"time"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var eventIgnoreReferrerColumn = columns.AlwaysNilEventColumn(
	columns.CoreInterfaces.EventIgnoreReferrer.ID,
	columns.CoreInterfaces.EventIgnoreReferrer.Field,
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Ignore Referrer",
		"Not applicable for Matomo protocol.",
	),
)

var eventDateUTCColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventDateUTC.ID,
	columns.CoreInterfaces.EventDateUTC.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		return event.BoundHit.MustParsedRequest().ServerReceivedTime.UTC().Format("2006-01-02"), nil
	},
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Event Date (UTC)",
		"The date when the event occurred in the UTC timezone, formatted as YYYY-MM-DD.",
	),
)

var eventTimestampUTCColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventTimestampUTC.ID,
	columns.CoreInterfaces.EventTimestampUTC.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		return event.BoundHit.MustParsedRequest().ServerReceivedTime.UTC().Format(time.RFC3339), nil
	},
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Event Timestamp (UTC)",
		"The precise UTC timestamp of when the event occurred, with second-level precision. This represents the time recorded when the hit is received by the server.", // nolint:lll // it's a description
	),
)

var eventPageReferrerColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageReferrer.ID,
	columns.CoreInterfaces.EventPageReferrer.Field,
	"urlref",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Page Referrer",
		"The URL of the page that referred the user to the current page, set to empty string for direct traffic or when referrer information is not available.", // nolint:lll // it's a description
	),
)

var eventPageTitleColumn = columns.FromQueryParamEventColumn(
	columns.CoreInterfaces.EventPageTitle.ID,
	columns.CoreInterfaces.EventPageTitle.Field,
	"action_name",
	columns.WithEventColumnRequired(false),
	columns.WithEventColumnDocs(
		"Page Title",
		"The title of the page where the event occurred, as specified in the action_name parameter.",
	),
)
