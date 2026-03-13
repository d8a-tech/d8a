package matomo

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var eventLinkURLColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventLinkURL.ID,
	ProtocolInterfaces.EventLinkURL.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		v := event.BoundHit.MustParsedRequest().QueryParams.Get("link")
		if v == "" {
			return nil, nil //nolint:nilnil // optional field
		}
		return v, nil
	},
	columns.WithEventColumnDocs(
		"Link URL",
		"The URL of an outbound link clicked by the user, extracted from the link query parameter.",
	),
)

var eventDownloadURLColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventDownloadURL.ID,
	ProtocolInterfaces.EventDownloadURL.Field,
	func(event *schema.Event) (any, schema.D8AColumnWriteError) {
		v := event.BoundHit.MustParsedRequest().QueryParams.Get("download")
		if v == "" {
			return nil, nil //nolint:nilnil // optional field
		}
		return v, nil
	},
	columns.WithEventColumnDocs(
		"Download URL",
		"The URL of a file downloaded by the user, extracted from the download query parameter.",
	),
)
