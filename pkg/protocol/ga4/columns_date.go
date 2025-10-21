package ga4

import (
	"strconv"
	"time"

	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
)

var eventTimestampUTCColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventTimestampUTC.ID,
	columns.CoreInterfaces.EventTimestampUTC.Field,
	func(event *schema.Event) (any, error) {
		return event.BoundHit.ServerReceivedTime.UTC().Format(time.RFC3339), nil
	},
)

var eventDateUTCColumn = columns.NewSimpleEventColumn(
	columns.CoreInterfaces.EventDateUTC.ID,
	columns.CoreInterfaces.EventDateUTC.Field,
	func(event *schema.Event) (any, error) {
		return event.BoundHit.ServerReceivedTime.UTC().Format("2006-01-02"), nil
	},
)

var eventPageLoadHashColumn = columns.NewSimpleEventColumn(
	ProtocolInterfaces.EventPageLoadHash.ID,
	ProtocolInterfaces.EventPageLoadHash.Field,
	func(event *schema.Event) (any, error) {
		_p := event.BoundHit.QueryParams.Get("_p")
		if _p == "" {
			return nil, nil // nolint:nilnil // nil is valid
		}
		pageLoadHash, err := strconv.ParseInt(_p, 10, 64)
		if err != nil {
			return nil, nil // nolint:nilnil // nil is valid
		}
		return time.UnixMilli(pageLoadHash).UTC().Format(time.RFC3339), nil
	},
)
