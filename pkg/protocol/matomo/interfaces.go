// Package matomo provides Matomo protocol specific column definitions.
package matomo

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// ProtocolInterfaces are the columns specific to the Matomo protocol.
var ProtocolInterfaces = struct {
	EventLinkURL              schema.Interface
	EventDownloadURL          schema.Interface
	EventSearchTerm           schema.Interface
	EventPreviousPageLocation schema.Interface
	EventNextPageLocation     schema.Interface
	EventPreviousPageTitle    schema.Interface
	EventNextPageTitle        schema.Interface
}{
	EventLinkURL: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/link_url",
		Field: &arrow.Field{Name: "link_url", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventDownloadURL: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/download_url",
		Field: &arrow.Field{Name: "download_url", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventSearchTerm: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/search_term",
		Field: &arrow.Field{Name: "search_term", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPreviousPageLocation: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/previous_page_location",
		Field: &arrow.Field{Name: "previous_page_location", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventNextPageLocation: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/next_page_location",
		Field: &arrow.Field{Name: "next_page_location", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventPreviousPageTitle: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/previous_page_title",
		Field: &arrow.Field{Name: "previous_page_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	EventNextPageTitle: schema.Interface{
		ID:    "matomo.protocols.d8a.tech/event/next_page_title",
		Field: &arrow.Field{Name: "next_page_title", Type: arrow.BinaryTypes.String, Nullable: true},
	},
}
