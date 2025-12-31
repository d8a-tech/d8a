package ga4

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
	"github.com/slipros/devicedetector"
)

// ColumnFromRawQueryParamOrDeviceInfo creates a new event colum, parsing raw value from
// query param or alternatively using device info from the user agent
func ColumnFromRawQueryParamOrDeviceInfo(
	interfaceID schema.InterfaceID,
	field *arrow.Field,
	queryParam string,
	deviceInfoFunc func(event *schema.Event, di *devicedetector.DeviceInfo) (any, error),
	options ...columns.EventColumnOptions,
) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		interfaceID,
		field,
		func(event *schema.Event) (any, error) {
			if queryParam != "" {
				paramV := event.BoundHit.MustServerAttributes().QueryParams.Get(queryParam)
				if paramV != "" {
					return paramV, nil
				}
			}
			ua, err := getDeviceInfo(event)
			if err != nil {
				logrus.Warnf(
					"%s: %v",
					interfaceID,
					err,
				)
				return nil, nil // nolint:nilnil // nil is valid
			}
			if ua == nil {
				return nil, nil // nolint:nilnil // nil is valid
			}
			return deviceInfoFunc(event, ua)
		},
		options...,
	)
}
