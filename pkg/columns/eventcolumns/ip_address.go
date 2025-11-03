// Package eventcolumns provides column implementations for event data tracking.
package eventcolumns

import (
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

// IPAddressColumn is the column for the IP address of an event
var IPAddressColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventIPAddress.ID,
	columns.CoreInterfaces.EventIPAddress.Field,
	func(event *schema.Event) (any, error) {
		return event.BoundHit.IP, nil
	},
	columns.WithEventColumnDocs(
		"IP Address",
		"The IP address (IPv4 or IPv6) from which the tracking request originates, used as the source for geolocation data.", // nolint:lll // it's a description
	),
)
