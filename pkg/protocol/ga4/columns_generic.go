package ga4

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/archbottle/dd2/pkg/detector"
	"github.com/d8a-tech/d8a/pkg/columns"
	"github.com/d8a-tech/d8a/pkg/schema"
	"github.com/sirupsen/logrus"
)

// ColumnFromRawQueryParamOrDeviceInfo creates a new event colum, parsing raw value from
// query param or alternatively using device info from the user agent
func ColumnFromRawQueryParamOrDeviceInfo(
	interfaceID schema.InterfaceID,
	field *arrow.Field,
	queryParam string,
	deviceInfoFunc func(event *schema.Event, result *detector.ParseResult) (any, schema.D8AColumnWriteError),
	options ...columns.EventColumnOptions,
) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		interfaceID,
		field,
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			if queryParam != "" {
				paramV := event.BoundHit.MustParsedRequest().QueryParams.Get(queryParam)
				if paramV != "" {
					return paramV, nil
				}
			}
			result, err := getDeviceInfo(event)
			if err != nil {
				logrus.Warnf(
					"%s: %v",
					interfaceID,
					err,
				)
				return nil, nil // nolint:nilnil // nil is valid
			}
			if result == nil {
				return nil, nil // nolint:nilnil // nil is valid
			}
			return deviceInfoFunc(event, result)
		},
		options...,
	)
}

// ColumnFromDeviceInfo creates a new event column, using device info from the user agent
func ColumnFromDeviceInfo(
	interfaceID schema.InterfaceID,
	field *arrow.Field,
	deviceInfoFunc func(event *schema.Event, result *detector.ParseResult) (any, schema.D8AColumnWriteError),
	options ...columns.EventColumnOptions,
) schema.EventColumn {
	return columns.NewSimpleEventColumn(
		interfaceID,
		field,
		func(event *schema.Event) (any, schema.D8AColumnWriteError) {
			result, err := getDeviceInfo(event)
			if err != nil {
				return nil, schema.NewBrokenEventError(
					fmt.Sprintf("failed to get device info: %v", err),
				)
			}
			if result == nil {
				return nil, nil // nolint:nilnil // nil is valid
			}
			return deviceInfoFunc(event, result)
		},
		options...,
	)
}
