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
		"The IP address from which the tracking request originated. Used as the source for geolocation data and can be used for fraud detection or analytics filtering.", // nolint:lll // it's a description
	),
)
